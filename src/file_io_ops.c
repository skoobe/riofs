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
#include "s3http_connection.h"
#include "cache_mng.h"
#include "utils.h"

/*{{{ struct */
struct _FileIO {
    Application *app;
    ConfData *conf;
    gchar *fname;

    // write
    size_t current_size;
    struct evbuffer *write_buf;
    gboolean multipart_initiated;
    gchar *uploadid;
    guint part_number;
    GList *l_parts; // list of FileIOPart

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

FileIO *fileio_create (Application *app, const gchar *fname)
{
    FileIO *fop;

    fop = g_new0 (FileIO, 1);
    fop->app = app;
    fop->conf = application_get_conf (app);
    fop->current_size = 0;
    fop->write_buf = evbuffer_new ();
    fop->fname = g_strdup_printf ("/%s", fname);
    fop->file_size = 0;
    fop->head_req_sent = FALSE;
    fop->multipart_initiated = FALSE;
    fop->uploadid = NULL;
    fop->l_parts = NULL;

    return fop;
}

void fileio_destroy (FileIO *fop)
{
    GList *l;
    
    for (l = g_list_first (fop->l_parts); l; l = g_list_next (l)) {
        FileIOPart *part = (FileIOPart *) l->data;
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

/*{{{ Complete Multipart Upload */
// multipart is sent
static void fileio_release_on_complete_cb (S3HttpConnection *con, void *ctx, gboolean success,
    const gchar *buf, size_t buf_len, 
    struct evkeyvalq *headers)
{   
    FileIO *fop = (FileIO *) ctx;
    
    s3http_connection_release (con);

    if (!success) {
        LOG_err (FIO_LOG, "Failed to send Multipart data to the server !");
        fileio_destroy (fop);
        return;
    }

    // done 
    LOG_debug (FIO_LOG, "Multipart Upload is done !");

    fileio_destroy (fop);
}

// got S3HttpConnection object
static void fileio_release_on_complete_con_cb (gpointer client, gpointer ctx)
{
    S3HttpConnection *con = (S3HttpConnection *) client;
    FileIO *fop = (FileIO *) ctx;
    gchar *path;
    gboolean res;
    struct evbuffer *xml_buf;
    GList *l;
    gchar *buf;

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

    s3http_connection_acquire (con);
    
    path = g_strdup_printf ("%s?uploadId=%s", 
        fop->fname, fop->uploadid);
    res = s3http_connection_make_request (con, 
        path, "POST", xml_buf,
        fileio_release_on_complete_cb,
        fop
    );
    g_free (path);
    evbuffer_free (xml_buf);

    if (!res) {
        LOG_err (FIO_LOG, "Failed to create HTTP request !");
        s3http_connection_release (con);
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

    if (!s3client_pool_get_client (application_get_write_client_pool (fop->app), 
        fileio_release_on_complete_con_cb, fop)) {
        LOG_err (FIO_LOG, "Failed to get HTTP client !");
        fileio_destroy (fop);
        return;
     }
}
/*}}}*/

/*{{{ sent part*/
// file is sent
static void fileio_release_on_part_sent_cb (S3HttpConnection *con, void *ctx, gboolean success,
    const gchar *buf, size_t buf_len, 
    struct evkeyvalq *headers)
{   
    FileIO *fop = (FileIO *) ctx;
    
    s3http_connection_release (con);

    if (!success) {
        LOG_err (FIO_LOG, "Failed to send bufer to server !");
        fileio_destroy (fop);
        return;
    }
    
    // if it's a multi part upload - Complete Multipart Upload
    if (fop->multipart_initiated) {
        fileio_release_complete_multipart (fop);
    
    // or we are done
    } else {
        fileio_destroy (fop);
    }
}

// got S3HttpConnection object
static void fileio_release_on_part_con_cb (gpointer client, gpointer ctx)
{
    S3HttpConnection *con = (S3HttpConnection *) client;
    FileIO *fop = (FileIO *) ctx;
    gchar *path;
    gboolean res;
    FileIOPart *part;
    size_t buf_len;
    char *buf;

    LOG_debug (FIO_LOG, "[s3http_con: %p] Releasing fop. Size: %zu", con, evbuffer_get_length (fop->write_buf));
    
    // add part information to the list
    part = g_new0 (FileIOPart, 1);
    part->part_number = fop->part_number;
    buf_len = evbuffer_get_length (fop->write_buf);
    buf = evbuffer_pullup (fop->write_buf, buf_len);
    get_md5_sum (buf, buf_len, &part->md5str, &part->md5b);
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
    
    s3http_connection_acquire (con);

    // add output headers
    s3http_connection_add_output_header (con, "Content-MD5", part->md5b);

    res = s3http_connection_make_request (con, 
        path, "PUT", fop->write_buf,
        fileio_release_on_part_sent_cb,
        fop
    );
    g_free (path);

    if (!res) {
        LOG_err (FIO_LOG, "Failed to create HTTP request !");
        s3http_connection_release (con);
        fileio_destroy (fop);
        return;
    }
}
/*}}}*/

// file is released, finish all operations
void fileio_release (FileIO *fop)
{
    // if write buffer has some data left - send it to the server
    if (evbuffer_get_length (fop->write_buf)) {
        if (!s3client_pool_get_client (application_get_write_client_pool (fop->app), 
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
static void fileio_write_on_send_cb (S3HttpConnection *con, void *ctx, gboolean success,
    const gchar *buf, size_t buf_len, 
    struct evkeyvalq *headers)
{
    FileWriteData *wdata = (FileWriteData *) ctx;
    
    s3http_connection_release (con);
    
    if (!success) {
        LOG_err (FIO_LOG, "Failed to send bufer to server !");
        wdata->on_buffer_written_cb (wdata->fop, wdata->ctx, FALSE, 0);
        g_free (wdata);
        return;
    }

    // done sending part
    wdata->on_buffer_written_cb (wdata->fop, wdata->ctx, TRUE, wdata->buf_size);
    g_free (wdata);
}

// got S3HttpConnection object
static void fileio_write_on_send_con_cb (gpointer client, gpointer ctx)
{
    S3HttpConnection *con = (S3HttpConnection *) client;
    FileWriteData *wdata = (FileWriteData *) ctx;
    gchar *path;
    gboolean res;
    FileIOPart *part;
    size_t buf_len;
    char *buf;

    s3http_connection_acquire (con);

    // add part information to the list
    part = g_new0 (FileIOPart, 1);
    part->part_number = wdata->fop->part_number;
    buf_len = evbuffer_get_length (wdata->fop->write_buf);
    buf = evbuffer_pullup (wdata->fop->write_buf, buf_len);
    get_md5_sum (buf, buf_len, &part->md5str, &part->md5b);
    wdata->fop->l_parts = g_list_append (wdata->fop->l_parts, part);

    path = g_strdup_printf ("%s?partNumber=%u&uploadId=%s", 
        wdata->fop->fname, wdata->fop->part_number, wdata->fop->uploadid);
    
    // increase part number
    wdata->fop->part_number++;
    // XXX: check that part_number does not exceeds 10000

    // add output headers
    s3http_connection_add_output_header (con, "Content-MD5", part->md5b);
    
    res = s3http_connection_make_request (con, 
        path, "PUT", wdata->fop->write_buf,
        fileio_write_on_send_cb,
        wdata
    );
    g_free (path);

    if (!res) {
        LOG_err (FIO_LOG, "Failed to create HTTP request !");
        s3http_connection_release (con);
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

    if (!s3client_pool_get_client (application_get_write_client_pool (wdata->fop->app), 
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
    nodes = uploadid_xp->nodesetval;

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

static void fileio_write_on_multipart_init_cb (S3HttpConnection *con, void *ctx, gboolean success,
    const gchar *buf, size_t buf_len, 
    struct evkeyvalq *headers)
{   
    FileWriteData *wdata = (FileWriteData *) ctx;
    gchar *uploadid;
    
    s3http_connection_release (con);
    
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

// got S3HttpConnection object
static void fileio_write_on_multipart_init_con_cb (gpointer client, gpointer ctx)
{
    S3HttpConnection *con = (S3HttpConnection *) client;
    FileWriteData *wdata = (FileWriteData *) ctx;
    gboolean res;
    gchar *path;

    s3http_connection_acquire (con);

    path = g_strdup_printf ("%s?uploads", wdata->fop->fname);
    res = s3http_connection_make_request (con, 
        path, "POST", NULL,
        fileio_write_on_multipart_init_cb,
        wdata
    );
    g_free (path);

    if (!res) {
        LOG_err (FIO_LOG, "Failed to create HTTP request !");
        s3http_connection_release (con);
        wdata->on_buffer_written_cb (wdata->fop, wdata->ctx, FALSE, 0);
        g_free (wdata);
        return;
    }
}

static void fileio_write_init_multipart (FileWriteData *wdata)
{
    if (!s3client_pool_get_client (application_get_write_client_pool (wdata->fop->app), 
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
    if (fop->current_size != off) {
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
    if (evbuffer_get_length (fop->write_buf) >= conf_get_uint (fop->conf, "s3.part_size")) {
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
    size_t size;
    off_t off;
    fuse_ino_t ino;
    off_t request_offset;
    FileIO_on_buffer_read_cb on_buffer_read_cb;
    gpointer ctx;
} FileReadData;

static void fileio_read_get_buf (FileReadData *rdata);

/*{{{ GET request */
static void fileio_read_on_get_cb (S3HttpConnection *con, void *ctx, gboolean success,
    const gchar *buf, size_t buf_len, 
    struct evkeyvalq *headers)
{   
    FileReadData *rdata = (FileReadData *) ctx;
    gchar *etag;
    
    // release S3HttpConnection
    s3http_connection_release (con);
   
    if (!success) {
        LOG_err (FIO_LOG, "Failed to get file from server !");
        rdata->on_buffer_read_cb (rdata->ctx, FALSE, NULL, 0);
        g_free (rdata);
        return;
    }

    etag = evhttp_find_header (headers, "ETag");
    if (etag && strlen (etag) > 3 && strlen (etag) == 32 + 2) { //Etag: "etag"
        gchar *md5;
        get_md5_sum (buf, buf_len, &md5, NULL);

        if (etag[0] == '"')
            etag = etag + 1;
        if (etag[strlen(etag) - 1] == '"')
            etag[strlen(etag) - 1] = '\0';

        if (strncmp (etag, md5, 32)) {
            LOG_err (FIO_LOG, "Local MD5 doesn't match Etag !");
            rdata->on_buffer_read_cb (rdata->ctx, FALSE, NULL, 0);
            g_free (rdata);
            return;
        }
    }

    
    // store it in the local cache
    cache_mng_store_file_buf (application_get_cache_mng (rdata->fop->app), 
        rdata->ino, buf_len, rdata->request_offset, (unsigned char *) buf,
        NULL, NULL);

    LOG_err (FIO_LOG, "Storing [%"G_GUINT64_FORMAT" %zu]", rdata->request_offset, buf_len);

    // and read it
    fileio_read_get_buf (rdata);
}

// got S3HttpConnection object
static void fileio_read_on_con_cb (gpointer client, gpointer ctx)
{
    S3HttpConnection *con = (S3HttpConnection *) client;
    FileReadData *rdata = (FileReadData *) ctx;
    gboolean res;
    guint64 part_size;

    s3http_connection_acquire (con);
    
    part_size = conf_get_uint (rdata->fop->conf, "s3.part_size");

    // small file - get the whole file at once
    if (rdata->fop->file_size < part_size)
        rdata->request_offset = 0;
    
    // calculate offset
    else {
        guint64 part_id;
        gchar *range_hdr;

        if (part_size < rdata->size)
            part_size = rdata->size;

        rdata->request_offset = rdata->off;
        range_hdr = g_strdup_printf ("bytes=%"G_GUINT64_FORMAT"-%"G_GUINT64_FORMAT, 
            rdata->request_offset, rdata->request_offset + part_size);
        s3http_connection_add_output_header (con, "Range", range_hdr);
        g_free (range_hdr);
    }
    
    res = s3http_connection_make_request (con, 
        rdata->fop->fname, "GET", NULL,
        fileio_read_on_get_cb,
        rdata
    );

    if (!res) {
        LOG_err (FIO_LOG, "Failed to create HTTP request !");
        s3http_connection_release (con);
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
        if (!s3client_pool_get_client (application_get_read_client_pool (rdata->fop->app), fileio_read_on_con_cb, rdata)) {
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
    if (rdata->fop->file_size && rdata->off + rdata->size > rdata->fop->file_size)
        rdata->size = rdata->fop->file_size - rdata->off;

    LOG_debug (FIO_LOG, "requesting [%"G_GUINT64_FORMAT" %zu]", rdata->off, rdata->size);

    cache_mng_retrieve_file_buf (application_get_cache_mng (rdata->fop->app), 
        rdata->ino, rdata->size, rdata->off,
        fileio_read_on_cache_cb, rdata);
}
/*}}}*/

/*{{{ HEAD request*/

static void fileio_read_on_head_cb (S3HttpConnection *con, void *ctx, gboolean success,
    const gchar *buf, size_t buf_len, 
    struct evkeyvalq *headers)
{   
    FileReadData *rdata = (FileReadData *) ctx;
    const char *content_len_header;
     
    // release S3HttpConnection
    s3http_connection_release (con);

    if (!success) {
        LOG_err (FIO_LOG, "Failed to get head from server !");
        rdata->on_buffer_read_cb (rdata->ctx, FALSE, NULL, 0);
        g_free (rdata);
        return;
    }

    rdata->fop->head_req_sent = TRUE;
    
    content_len_header = evhttp_find_header (headers, "Content-Length");
    if (content_len_header) {
        rdata->fop->file_size = strtoll ((char *)content_len_header, NULL, 10);
    }
    //XXX : MD5
    
    // resume downloading file
    fileio_read_get_buf (rdata);
}

// got S3HttpConnection object
static void fileio_read_on_head_con_cb (gpointer client, gpointer ctx)
{
    S3HttpConnection *con = (S3HttpConnection *) client;
    FileReadData *rdata = (FileReadData *) ctx;
    gboolean res;

    s3http_connection_acquire (con);

    res = s3http_connection_make_request (con, 
        rdata->fop->fname, "HEAD", NULL,
        fileio_read_on_head_cb,
        rdata
    );

    if (!res) {
        LOG_err (FIO_LOG, "Failed to create HTTP request !");
        s3http_connection_release (con);
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
        if (!s3client_pool_get_client (application_get_read_client_pool (rdata->fop->app), fileio_read_on_head_con_cb, rdata)) {
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
