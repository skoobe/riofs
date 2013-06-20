/*
 * Copyright (C) 2012-2013 Paul Ionkin <paul.ionkin@gmail.com>
 * Copyright (C) 2012-2013 Skoobe GmbH. All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */
#include "file_io_ops.h"
#include "http_connection.h"
#include "cache_mng.h"
#include "utils.h"

/*{{{ struct */
struct _FileIO {
    Application *app;
    gchar *fname;
    fuse_ino_t ino;
    gboolean assume_new; // assume file does not exist yet

    // write
    guint64 current_size;
    struct evbuffer *write_buf;
    gboolean multipart_initiated;
    gchar *uploadid;
    guint part_number;
    GList *l_parts; // list of FileIOPart
    MD5_CTX md5;

    // read
    gboolean head_req_sent;
    guint64 file_size;
};

typedef struct {
    guint part_number;
    gchar *md5str;
    gchar *md5b;
} FileIOPart;
/*}}}*/

#define FIO_LOG "fio"

/*{{{ create / destroy */

FileIO *fileio_create (Application *app, const gchar *fname, fuse_ino_t ino, gboolean assume_new)
{
    FileIO *fop;

    fop = g_new0 (FileIO, 1);
    fop->app = app;
    fop->current_size = 0;
    fop->write_buf = evbuffer_new ();
    fop->fname = g_strdup_printf ("/%s", fname);
    fop->file_size = 0;
    fop->head_req_sent = FALSE;
    fop->multipart_initiated = FALSE;
    fop->uploadid = NULL;
    fop->l_parts = NULL;
    fop->ino = ino;
    fop->assume_new = assume_new;
    MD5_Init (&fop->md5);

    return fop;
}

void fileio_destroy (FileIO *fop)
{
    GList *l;
    
    for (l = g_list_first (fop->l_parts); l; l = g_list_next (l)) {
        FileIOPart *part = (FileIOPart *) l->data;
        g_free (part->md5str);
        g_free (part->md5b);
        g_free (part);
    }
    evbuffer_free (fop->write_buf);
    g_free (fop->fname);
    if (fop->uploadid)
        g_free (fop->uploadid);
    g_free (fop);
}
/*}}}*/

/*{{{ fileio_release*/

/*{{{ update headers on uploaded object */
static void fileio_release_on_update_header_cb (HttpConnection *con, void *ctx, gboolean success,
    G_GNUC_UNUSED const gchar *buf, G_GNUC_UNUSED size_t buf_len, 
    G_GNUC_UNUSED struct evkeyvalq *headers)
{   
    FileIO *fop = (FileIO *) ctx;
    
    http_connection_release (con);

    if (!success) {
        LOG_err (FIO_LOG, "Failed to update headers on the server !");
        fileio_destroy (fop);
        return;
    }

    // done 
    LOG_debug (FIO_LOG, "Headers are updated !");

    fileio_destroy (fop);
}

// got HttpConnection object
static void fileio_release_on_update_headers_con_cb (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    FileIO *fop = (FileIO *) ctx;
    gchar *path;
    gchar *cpy_path;
    gboolean res;
    unsigned char digest[16];
    gchar *md5str;
    size_t i;

    LOG_debug (FIO_LOG, "Updating object's headers..");

    http_connection_acquire (con);
    
    http_connection_add_output_header (con, "x-amz-metadata-directive", "REPLACE");
    
    MD5_Final (digest, &fop->md5);
    md5str = g_malloc (33);
    for (i = 0; i < 16; ++i)
        sprintf(&md5str[i*2], "%02x", (unsigned int)digest[i]);
    http_connection_add_output_header (con, "x-amz-meta-md5", md5str);
    g_free (md5str);

    cpy_path = g_strdup_printf ("%s%s", conf_get_string (application_get_conf (fop->app), "s3.bucket_name"), fop->fname);
    http_connection_add_output_header (con, "x-amz-copy-source", cpy_path);
    g_free (cpy_path);

    path = g_strdup_printf ("%s", fop->fname);
    res = http_connection_make_request (con, 
        path, "PUT", NULL,
        fileio_release_on_update_header_cb,
        fop
    );
    g_free (path);

    if (!res) {
        LOG_err (FIO_LOG, "Failed to create HTTP request !");
        http_connection_release (con);
        fileio_destroy (fop);
        return;
    }
}

static void fileio_release_update_headers (FileIO *fop)
{
    // update MD5 headers only if versioning is disabled
    if (conf_get_boolean (application_get_conf (fop->app), "s3.versioning")) {
        LOG_debug (FIO_LOG, "File uploaded !");
        fileio_destroy (fop);
    } else {
        if (!client_pool_get_client (application_get_write_client_pool (fop->app), 
            fileio_release_on_update_headers_con_cb, fop)) {
            LOG_err (FIO_LOG, "Failed to get HTTP client !");
            fileio_destroy (fop);
            return;
        }
    }
}
/*}}}*/

/*{{{ Complete Multipart Upload */
// multipart is sent
static void fileio_release_on_complete_cb (HttpConnection *con, void *ctx, gboolean success,
    G_GNUC_UNUSED const gchar *buf, G_GNUC_UNUSED size_t buf_len, 
    G_GNUC_UNUSED struct evkeyvalq *headers)
{   
    FileIO *fop = (FileIO *) ctx;
    const gchar *versioning_header;
    
    http_connection_release (con);

    if (!success) {
        LOG_err (FIO_LOG, "Failed to send Multipart data to the server !");
        fileio_destroy (fop);
        return;
    }

    versioning_header = http_find_header (headers, "x-amz-version-id");
    if (versioning_header) {
        cache_mng_update_version_id (application_get_cache_mng (fop->app), 
            fop->ino, versioning_header);
    }

    // done 
    LOG_debug (FIO_LOG, "Multipart Upload is done !");

    // fileio_destroy (fop);
    fileio_release_update_headers (fop);
}

// got HttpConnection object
static void fileio_release_on_complete_con_cb (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    FileIO *fop = (FileIO *) ctx;
    gchar *path;
    gboolean res;
    struct evbuffer *xml_buf;
    GList *l;

    xml_buf = evbuffer_new ();
    evbuffer_add_printf (xml_buf, "%s", "<CompleteMultipartUpload>");
    for (l = g_list_first (fop->l_parts); l; l = g_list_next (l)) {
        FileIOPart *part = (FileIOPart *) l->data;
        evbuffer_add_printf (xml_buf, 
            "<Part><PartNumber>%u</PartNumber><ETag>\"%s\"</ETag></Part>",
            part->part_number, part->md5str);
    }
    evbuffer_add_printf (xml_buf, "%s", "</CompleteMultipartUpload>");

    LOG_debug (FIO_LOG, "Sending Multipart Final part..");

    http_connection_acquire (con);
    
    path = g_strdup_printf ("%s?uploadId=%s", 
        fop->fname, fop->uploadid);
    res = http_connection_make_request (con, 
        path, "POST", xml_buf,
        fileio_release_on_complete_cb,
        fop
    );
    g_free (path);
    evbuffer_free (xml_buf);

    if (!res) {
        LOG_err (FIO_LOG, "Failed to create HTTP request !");
        http_connection_release (con);
        fileio_destroy (fop);
        return;
    }
}

static void fileio_release_complete_multipart (FileIO *fop)
{
    if (!fop->uploadid) {
        LOG_err (FIO_LOG, "UploadID is not set, aborting operation !");
        fileio_destroy (fop);
        return;
    }

    if (!client_pool_get_client (application_get_write_client_pool (fop->app), 
        fileio_release_on_complete_con_cb, fop)) {
        LOG_err (FIO_LOG, "Failed to get HTTP client !");
        fileio_destroy (fop);
        return;
     }
}
/*}}}*/

/*{{{ sent part*/
// file is sent
static void fileio_release_on_part_sent_cb (HttpConnection *con, void *ctx, gboolean success,
    G_GNUC_UNUSED const gchar *buf, G_GNUC_UNUSED size_t buf_len, 
    G_GNUC_UNUSED struct evkeyvalq *headers)
{   
    FileIO *fop = (FileIO *) ctx;
    const gchar *versioning_header;
    
    http_connection_release (con);

    if (!success) {
        LOG_err (FIO_LOG, "Failed to send bufer to server !");
        fileio_destroy (fop);
        return;
    }
    
    versioning_header = http_find_header (headers, "x-amz-version-id");
    if (versioning_header) {
        cache_mng_update_version_id (application_get_cache_mng (fop->app), 
            fop->ino, versioning_header);
    }
    // if it's a multi part upload - Complete Multipart Upload
    if (fop->multipart_initiated) {
        fileio_release_complete_multipart (fop);
    
    // or we are done
    } else {
        fileio_release_update_headers (fop);
        //fileio_destroy (fop);
    }
}

// got HttpConnection object
static void fileio_release_on_part_con_cb (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    FileIO *fop = (FileIO *) ctx;
    gchar *path;
    gboolean res;
    FileIOPart *part;
    size_t buf_len;
    const gchar *buf;

    LOG_debug (FIO_LOG, "[http_con: %p] Releasing fop. Size: %zu", con, evbuffer_get_length (fop->write_buf));
    
    // add part information to the list
    part = g_new0 (FileIOPart, 1);
    part->part_number = fop->part_number;
    buf_len = evbuffer_get_length (fop->write_buf);
    buf = (const gchar *)evbuffer_pullup (fop->write_buf, buf_len);
    
    // XXX: move to separate thread
    // 1. calculate MD5 of a part.
    get_md5_sum (buf, buf_len, &part->md5str, &part->md5b);
    // 2. calculate MD5 of multiple message blocks
    MD5_Update (&fop->md5, buf, buf_len);

    fop->l_parts = g_list_append (fop->l_parts, part);

    // if this is a multipart 
    if (fop->multipart_initiated) {
   
        if (!fop->uploadid) {
            LOG_err (FIO_LOG, "UploadID is not set, aborting operation !");
            fileio_destroy (fop);
            return;
        }

        path = g_strdup_printf ("%s?partNumber=%u&uploadId=%s", 
            fop->fname, fop->part_number, fop->uploadid);
        fop->part_number++;

    } else {
        path = g_strdup (fop->fname);
    }
    
    http_connection_acquire (con);

    // add output headers
    http_connection_add_output_header (con, "Content-MD5", part->md5b);

    res = http_connection_make_request (con, 
        path, "PUT", fop->write_buf,
        fileio_release_on_part_sent_cb,
        fop
    );
    g_free (path);

    if (!res) {
        LOG_err (FIO_LOG, "Failed to create HTTP request !");
        http_connection_release (con);
        fileio_destroy (fop);
        return;
    }
}
/*}}}*/

// file is released, finish all operations
void fileio_release (FileIO *fop)
{
    // if write buffer has some data left - send it to the server
    // or an empty file was created
    if (evbuffer_get_length (fop->write_buf) || fop->assume_new) {
        if (!client_pool_get_client (application_get_write_client_pool (fop->app), 
            fileio_release_on_part_con_cb, fop)) {
            LOG_err (FIO_LOG, "Failed to get HTTP client !");
            fileio_destroy (fop);
            return;
        }
    } else {
        // if it's a multi part upload - Complete Multipart Upload
        if (fop->multipart_initiated) {
            fileio_release_complete_multipart (fop);

        // just a "small" file
        } else 
            fileio_destroy (fop);
    }
}
/*}}}*/

/*{{{ fileio_write_buffer */

typedef struct {
    FileIO *fop;
    size_t buf_size;
    off_t off;
    fuse_ino_t ino;
    FileIO_on_buffer_written_cb on_buffer_written_cb;
    gpointer ctx;
} FileWriteData;

/*{{{ send part */

// buffer is sent
static void fileio_write_on_send_cb (HttpConnection *con, void *ctx, gboolean success,
    G_GNUC_UNUSED const gchar *buf, G_GNUC_UNUSED size_t buf_len, 
    G_GNUC_UNUSED struct evkeyvalq *headers)
{
    FileWriteData *wdata = (FileWriteData *) ctx;
    const char *versioning_header;
    
    http_connection_release (con);
    
    if (!success) {
        LOG_err (FIO_LOG, "Failed to send bufer to server !");
        wdata->on_buffer_written_cb (wdata->fop, wdata->ctx, FALSE, 0);
        g_free (wdata);
        return;
    }

    versioning_header = http_find_header (headers, "x-amz-version-id");
    if (versioning_header) {
        cache_mng_update_version_id (application_get_cache_mng (wdata->fop->app), 
            wdata->ino, versioning_header);
    }

    // done sending part
    wdata->on_buffer_written_cb (wdata->fop, wdata->ctx, TRUE, wdata->buf_size);
    g_free (wdata);
}

// got HttpConnection object
static void fileio_write_on_send_con_cb (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    FileWriteData *wdata = (FileWriteData *) ctx;
    gchar *path;
    gboolean res;
    FileIOPart *part;
    size_t buf_len;
    const gchar *buf;

    http_connection_acquire (con);

    // add part information to the list
    part = g_new0 (FileIOPart, 1);
    part->part_number = wdata->fop->part_number;
    buf_len = evbuffer_get_length (wdata->fop->write_buf);
    buf = (const gchar *) evbuffer_pullup (wdata->fop->write_buf, buf_len);
    
    // XXX: move to separate thread
    // 1. calculate MD5 of a part.
    get_md5_sum (buf, buf_len, &part->md5str, &part->md5b);
    // 2. calculate MD5 of multiple message blocks
    MD5_Update (&wdata->fop->md5, buf, buf_len);

    wdata->fop->l_parts = g_list_append (wdata->fop->l_parts, part);

    path = g_strdup_printf ("%s?partNumber=%u&uploadId=%s", 
        wdata->fop->fname, wdata->fop->part_number, wdata->fop->uploadid);
   
    // increase part number
    wdata->fop->part_number++;
    // XXX: check that part_number does not exceeds 10000

    // add output headers
    http_connection_add_output_header (con, "Content-MD5", part->md5b);
    
    res = http_connection_make_request (con, 
        path, "PUT", wdata->fop->write_buf,
        fileio_write_on_send_cb,
        wdata
    );
    g_free (path);

    if (!res) {
        LOG_err (FIO_LOG, "Failed to create HTTP request !");
        http_connection_release (con);
        wdata->on_buffer_written_cb (wdata->fop, wdata->ctx, FALSE, 0);
        g_free (wdata);
        return;
    }
}

static void fileio_write_send_part (FileWriteData *wdata)
{
    if (!wdata->fop->uploadid) {
        LOG_err (FIO_LOG, "UploadID is not set, aborting operation !");
        wdata->on_buffer_written_cb (wdata->fop, wdata->ctx, FALSE, 0);
        g_free (wdata);
        return;
    }

    if (!client_pool_get_client (application_get_write_client_pool (wdata->fop->app), 
        fileio_write_on_send_con_cb, wdata)) {
        LOG_err (FIO_LOG, "Failed to get HTTP client !");
        wdata->on_buffer_written_cb (wdata->fop, wdata->ctx, FALSE, 0);
        g_free (wdata);
        return;
    }
}
/*}}}*/

/*{{{ Multipart Init */

static gchar *get_uploadid (const char *xml, size_t xml_len) {
    xmlDocPtr doc;
    xmlXPathContextPtr ctx;
    xmlXPathObjectPtr uploadid_xp;
    xmlNodeSetPtr nodes;
    gchar *uploadid = NULL;

    doc = xmlReadMemory (xml, xml_len, "", NULL, 0);
    ctx = xmlXPathNewContext (doc);
    xmlXPathRegisterNs (ctx, (xmlChar *) "s3", (xmlChar *) "http://s3.amazonaws.com/doc/2006-03-01/");

    uploadid_xp = xmlXPathEvalExpression ((xmlChar *) "//s3:UploadId", ctx);
    if (!uploadid_xp) {
        LOG_err (FIO_LOG, "S3 returned incorrect XML !");
        xmlXPathFreeContext (ctx);
        xmlFreeDoc (doc);
        return NULL;
    }

    nodes = uploadid_xp->nodesetval;
    if (!nodes) {
        LOG_err (FIO_LOG, "S3 returned incorrect XML !");
        xmlXPathFreeObject (uploadid_xp);
        xmlXPathFreeContext (ctx);
        xmlFreeDoc (doc);
        return NULL;
    }

    if (!nodes || nodes->nodeNr < 1) {
        uploadid = NULL;
    } else {
        uploadid = (char *) xmlNodeListGetString (doc, nodes->nodeTab[0]->xmlChildrenNode, 1);
    }

    xmlXPathFreeObject (uploadid_xp);
    xmlXPathFreeContext (ctx);
    xmlFreeDoc (doc);

    return uploadid;
}

static void fileio_write_on_multipart_init_cb (HttpConnection *con, void *ctx, gboolean success,
    const gchar *buf, size_t buf_len, 
    G_GNUC_UNUSED struct evkeyvalq *headers)
{   
    FileWriteData *wdata = (FileWriteData *) ctx;
    gchar *uploadid;
    
    http_connection_release (con);
    
    wdata->fop->multipart_initiated = TRUE;
    
    if (!success || !buf_len) {
        LOG_err (FIO_LOG, "Failed to get multipart init data from the server !");
        wdata->on_buffer_written_cb (wdata->fop, wdata->ctx, FALSE, 0);
        g_free (wdata);
        return;
    }

    uploadid = get_uploadid (buf, buf_len);
    if (!uploadid) {
        LOG_err (FIO_LOG, "Failed to parse multipart init data!");
        wdata->on_buffer_written_cb (wdata->fop, wdata->ctx, FALSE, 0);
        g_free (wdata);
        return;
    }
    wdata->fop->uploadid = g_strdup (uploadid);
    xmlFree (uploadid);

    // done, resume uploading part
    wdata->fop->part_number = 1;
    fileio_write_send_part (wdata);
}

// got HttpConnection object
static void fileio_write_on_multipart_init_con_cb (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    FileWriteData *wdata = (FileWriteData *) ctx;
    gboolean res;
    gchar *path;

    http_connection_acquire (con);

    path = g_strdup_printf ("%s?uploads", wdata->fop->fname);
    res = http_connection_make_request (con, 
        path, "POST", NULL,
        fileio_write_on_multipart_init_cb,
        wdata
    );
    g_free (path);

    if (!res) {
        LOG_err (FIO_LOG, "Failed to create HTTP request !");
        http_connection_release (con);
        wdata->on_buffer_written_cb (wdata->fop, wdata->ctx, FALSE, 0);
        g_free (wdata);
        return;
    }
}

static void fileio_write_init_multipart (FileWriteData *wdata)
{
    if (!client_pool_get_client (application_get_write_client_pool (wdata->fop->app), 
        fileio_write_on_multipart_init_con_cb, wdata)) {
        LOG_err (FIO_LOG, "Failed to get HTTP client !");
        wdata->on_buffer_written_cb (wdata->fop, wdata->ctx, FALSE, 0);
        g_free (wdata);
        return;
    }
}
/*}}}*/

void fileio_write_buffer (FileIO *fop,
    const char *buf, size_t buf_size, off_t off, fuse_ino_t ino,
    FileIO_on_buffer_written_cb on_buffer_written_cb, gpointer ctx)
{
    FileWriteData *wdata;

    // XXX: allow only sequentially write
    // current written bytes should be always match offset
    if (off >= 0 && fop->current_size != (guint64)off) {
        LOG_err (FIO_LOG, "Write call with offset %"OFF_FMT" is not allowed !", off);
        on_buffer_written_cb (fop, ctx, FALSE, 0);
        return;
    }

    // add data to output buffer
    evbuffer_add (fop->write_buf, buf, buf_size);
    fop->current_size += buf_size;

    // CacheMng
    cache_mng_store_file_buf (application_get_cache_mng (fop->app), 
        ino, buf_size, off, (unsigned char *) buf, 
        NULL, NULL);
      
    // if current write buffer exceeds "part_size" - this is a multipart upload
    if (evbuffer_get_length (fop->write_buf) >= conf_get_uint (application_get_conf (fop->app), "s3.part_size")) {
        // init helper struct
        wdata = g_new0 (FileWriteData, 1);
        wdata->fop = fop;
        wdata->buf_size = buf_size;
        wdata->off = off;
        wdata->ino = ino;
        wdata->on_buffer_written_cb = on_buffer_written_cb;
        wdata->ctx = ctx;

        // init multipart upload
        if (!fop->multipart_initiated) {
            fileio_write_init_multipart (wdata);

        // else send the current part
        } else {
            fileio_write_send_part (wdata);
        }
    
    // or just notify client that we are ready for more data
    } else {
        on_buffer_written_cb (fop, ctx, TRUE, buf_size);
    }
}
/*}}}*/

/*{{{ fileio_read_buffer*/

typedef struct {
    FileIO *fop;
    guint64 size;
    off_t off;
    fuse_ino_t ino;
    off_t request_offset;
    FileIO_on_buffer_read_cb on_buffer_read_cb;
    gpointer ctx;
} FileReadData;

static void fileio_read_get_buf (FileReadData *rdata);

/*{{{ GET request */
static void fileio_read_on_get_cb (HttpConnection *con, void *ctx, gboolean success,
    const gchar *buf, size_t buf_len, 
    G_GNUC_UNUSED struct evkeyvalq *headers)
{   
    FileReadData *rdata = (FileReadData *) ctx;
    const char *versioning_header = NULL;
    
    // release HttpConnection
    http_connection_release (con);
   
    if (!success) {
        LOG_err (FIO_LOG, "Failed to get file from server !");
        rdata->on_buffer_read_cb (rdata->ctx, FALSE, NULL, 0);
        g_free (rdata);
        return;
    }

    // store it in the local cache
    cache_mng_store_file_buf (application_get_cache_mng (rdata->fop->app), 
        rdata->ino, buf_len, rdata->request_offset, (unsigned char *) buf,
        NULL, NULL);

    // update version ID
    versioning_header = http_find_header (headers, "x-amz-version-id");
    if (versioning_header) {
        cache_mng_update_version_id (application_get_cache_mng (rdata->fop->app), rdata->ino, versioning_header);
    }

    LOG_debug (FIO_LOG, "Storing [%"G_GUINT64_FORMAT" %zu]", rdata->request_offset, buf_len);

    // and read it
    fileio_read_get_buf (rdata);
}

// got HttpConnection object
static void fileio_read_on_con_cb (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    FileReadData *rdata = (FileReadData *) ctx;
    gboolean res;
    guint64 part_size;

    http_connection_acquire (con);
    
    part_size = conf_get_uint (application_get_conf (rdata->fop->app), "s3.part_size");

    // small file - get the whole file at once
    if (rdata->fop->file_size < part_size)
        rdata->request_offset = 0;
    
    // calculate offset
    else {
        gchar *range_hdr;

        if (part_size < rdata->size)
            part_size = rdata->size;

        rdata->request_offset = rdata->off;
        range_hdr = g_strdup_printf ("bytes=%"G_GUINT64_FORMAT"-%"G_GUINT64_FORMAT, 
            rdata->request_offset, rdata->request_offset + part_size);
        http_connection_add_output_header (con, "Range", range_hdr);
        g_free (range_hdr);
    }
    
    res = http_connection_make_request (con, 
        rdata->fop->fname, "GET", NULL,
        fileio_read_on_get_cb,
        rdata
    );

    if (!res) {
        LOG_err (FIO_LOG, "Failed to create HTTP request !");
        http_connection_release (con);
        rdata->on_buffer_read_cb (rdata->ctx, FALSE, NULL, 0);
        g_free (rdata);
        return;
    }
}

static void fileio_read_on_cache_cb (unsigned char *buf, size_t size, gboolean success, void *ctx)
{
    FileReadData *rdata = (FileReadData *) ctx;
    
    // we got data from the cache
    if (success) {
        LOG_debug (FIO_LOG, "Reading from cache");
        rdata->on_buffer_read_cb (rdata->ctx, TRUE, (char *)buf, size);
        g_free (rdata);
    } else {
        LOG_debug (FIO_LOG, "Reading from server !!");
        if (!client_pool_get_client (application_get_read_client_pool (rdata->fop->app), fileio_read_on_con_cb, rdata)) {
            LOG_err (FIO_LOG, "Failed to get HTTP client !");
            rdata->on_buffer_read_cb (rdata->ctx, FALSE, NULL, 0);
            g_free (rdata);
            return;
        }
    }
}

static void fileio_read_get_buf (FileReadData *rdata)
{
    // make sure request does not exceed file size
    if (rdata->fop->file_size > 0 && (guint64) (rdata->off + rdata->size) > rdata->fop->file_size) {
        rdata->size = rdata->fop->file_size - rdata->off;
    // special case, when zero-size file is reqeusted
    } else if (rdata->fop->file_size == 0) {
        rdata->size = 0;
    }

    LOG_debug (FIO_LOG, "requesting [%"G_GUINT64_FORMAT" %zu]", rdata->off, rdata->size);

    cache_mng_retrieve_file_buf (application_get_cache_mng (rdata->fop->app), 
        rdata->ino, rdata->size, rdata->off,
        fileio_read_on_cache_cb, rdata);
}
/*}}}*/

/*{{{ HEAD request*/

static void fileio_read_on_head_cb (HttpConnection *con, void *ctx, gboolean success,
    G_GNUC_UNUSED const gchar *buf, G_GNUC_UNUSED size_t buf_len, 
    struct evkeyvalq *headers)
{   
    FileReadData *rdata = (FileReadData *) ctx;
    const char *content_len_header;
     
    // release HttpConnection
    http_connection_release (con);

    if (!success) {
        LOG_err (FIO_LOG, "Failed to get head from server !");
        rdata->on_buffer_read_cb (rdata->ctx, FALSE, NULL, 0);
        g_free (rdata);
        return;
    }

    rdata->fop->head_req_sent = TRUE;
    
    // consistency checking:

    // 1. check local and remote file sizes
    content_len_header = http_find_header (headers, "Content-Length");
    if (content_len_header) {
        guint64 local_size = 0;
        gint64 size = 0;

        size = strtoll ((char *)content_len_header, NULL, 10);
        if (size < 0) {
            LOG_err (FIO_LOG, "Header contains incorrect file size!");
            size = 0;
        }

        rdata->fop->file_size = size;
        LOG_debug (FIO_LOG, "Remote file size: %"G_GUINT64_FORMAT, rdata->fop->file_size);
        
        local_size = cache_mng_get_file_length (application_get_cache_mng (rdata->fop->app), rdata->ino);
        if (local_size != rdata->fop->file_size) {
            LOG_debug (FIO_LOG, "Local and remote file sizes do not match, invalidating local cached file!");
            cache_mng_remove_file (application_get_cache_mng (rdata->fop->app), rdata->ino);
        }
    }
    
    // 2. use one of the following ways to check that local and remote files are identical
    // if versioning is enabled: compare version IDs
    // if bucket has versioning disabled: compare MD5 sums
    if (conf_get_boolean (application_get_conf (rdata->fop->app), "s3.versioning")) {
        const char *versioning_header = http_find_header (headers, "x-amz-version-id");
        if (versioning_header) {
            const gchar *local_version_id = cache_mng_get_version_id (application_get_cache_mng (rdata->fop->app), rdata->ino);
            if (local_version_id && !strcmp (local_version_id, versioning_header)) {
                LOG_debug (FIO_LOG, "Both version IDs match, using local cached file!");
            } else {
                LOG_debug (FIO_LOG, "Version IDs do not match, invalidating local cached file!: %s %s", 
                        local_version_id, versioning_header);
                cache_mng_remove_file (application_get_cache_mng (rdata->fop->app), rdata->ino);
            }

        // header was not found
        } else {
            LOG_debug (FIO_LOG, "Versioning header was not found, invalidating local cached file!");
            cache_mng_remove_file (application_get_cache_mng (rdata->fop->app), rdata->ino);
        }
    
    //check for MD5
    } else  {
        const char *md5_header = http_find_header (headers, "x-amz-meta-md5");
        if (md5_header) {
            gchar *md5str = NULL;

            // at this point we have both remote and local MD5 sums
            if (cache_mng_get_md5 (application_get_cache_mng (rdata->fop->app), rdata->ino, &md5str)) {
                if (!strncmp (md5_header, md5str, 32)) {
                    LOG_debug (FIO_LOG, "MD5 sums match, using local cached file!");
                } else {
                    LOG_debug (FIO_LOG, "MD5 sums do not match, invalidating local cached file!");
                    cache_mng_remove_file (application_get_cache_mng (rdata->fop->app), rdata->ino);
                }
            } else {
                LOG_debug (FIO_LOG, "Failed to get local MD5 sum, invalidating local cached file!");
                cache_mng_remove_file (application_get_cache_mng (rdata->fop->app), rdata->ino);
            }

            if (md5str)
                g_free (md5str);

        // header was not found
        } else {
            LOG_debug (FIO_LOG, "MD5 sum header was not found, invalidating local cached file!");
            cache_mng_remove_file (application_get_cache_mng (rdata->fop->app), rdata->ino);
        }
    }
    
    // resume downloading file
    fileio_read_get_buf (rdata);
}

// got HttpConnection object
static void fileio_read_on_head_con_cb (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    FileReadData *rdata = (FileReadData *) ctx;
    gboolean res;

    http_connection_acquire (con);

    res = http_connection_make_request (con, 
        rdata->fop->fname, "HEAD", NULL,
        fileio_read_on_head_cb,
        rdata
    );

    if (!res) {
        LOG_err (FIO_LOG, "Failed to create HTTP request !");
        http_connection_release (con);
        rdata->on_buffer_read_cb (rdata->ctx, FALSE, NULL, 0);
        g_free (rdata);
        return;
    }
}
/*}}}*/

// if it's the first fuse read() request - send HEAD request to server
// else try to get data from local cache, otherwise download from the server
void fileio_read_buffer (FileIO *fop,
    size_t size, off_t off, fuse_ino_t ino,
    FileIO_on_buffer_read_cb on_buffer_read_cb, gpointer ctx)
{
    FileReadData *rdata;

    rdata = g_new0 (FileReadData, 1);
    rdata->fop = fop;
    rdata->size = size;
    rdata->off = off;
    rdata->ino = ino;
    rdata->on_buffer_read_cb = on_buffer_read_cb;
    rdata->ctx = ctx;
    rdata->request_offset = off;
    
    // send HEAD request first
    if (!rdata->fop->head_req_sent) {
         // get HTTP connection to download manifest or a full file
        if (!client_pool_get_client (application_get_read_client_pool (rdata->fop->app), fileio_read_on_head_con_cb, rdata)) {
            LOG_err (FIO_LOG, "Failed to get HTTP client !");
            rdata->on_buffer_read_cb (rdata->ctx, FALSE, NULL, 0);
            g_free (rdata);
        }

    // HEAD is sent, try to get data from cache
    } else {
        fileio_read_get_buf (rdata);
    }
}
/*}}}*/
