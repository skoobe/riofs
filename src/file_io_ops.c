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

/*{{{ struct */
struct _FileIO {
    Application *app;
    ConfData *conf;

    // write
    size_t current_size;
    struct evbuffer *write_buf;
    gchar *fname;

    // read
    gboolean head_req_sent;
    guint64 file_size;
    gboolean tmp_xx;
};
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
    fop->tmp_xx = FALSE;

    return fop;
}

void fileio_destroy (FileIO *fop)
{
    evbuffer_free (fop->write_buf);
    g_free (fop->fname);
    g_free (fop);
}
/*}}}*/

/*{{{ fileio_release*/

// file is sent
static void fileio_release_on_sent_cb (S3HttpConnection *con, void *ctx, gboolean success,
    const gchar *buf, size_t buf_len, 
    struct evkeyvalq *headers)
{   
    FileIO *fop = (FileIO *) ctx;
    
    if (!success)
        LOG_err (FIO_LOG, "Failed to send bufer to server !");
    else
        LOG_debug (FIO_LOG, "File sent !");

    // release S3HttpConnection
    s3http_connection_release (con);
    fileio_release (fop);
}

// got S3HttpConnection object
static void fileio_release_on_http_client_cb (gpointer client, gpointer ctx)
{
    S3HttpConnection *con = (S3HttpConnection *) client;
    FileIO *fop = (FileIO *) ctx;
    gboolean res;

    LOG_debug (FIO_LOG, "[s3http_con: %p] Releasing fop. Size: %zu", con, evbuffer_get_length (fop->write_buf));
    
    s3http_connection_acquire (con);

    res = s3http_connection_make_request (con, 
        fop->fname, "PUT", fop->write_buf,
        fileio_release_on_sent_cb,
        fop
    );

    if (!res) {
        LOG_err (FIO_LOG, "Failed to create HTTP request !");
        s3http_connection_release (con);
        fileio_release (fop);
        return;
    }
}

// file is released, finish all operations
void fileio_release (FileIO *fop)
{
    // if write buffer has some data left - send it to the server
    if (evbuffer_get_length (fop->write_buf)) {
        if (!s3client_pool_get_client (application_get_write_client_pool (fop->app), fileio_release_on_http_client_cb, fop)) {
            LOG_err (FIO_LOG, "Failed to get HTTP client !");
            return;
        }
    } else {
        fileio_destroy (fop);
    }
}
/*}}}*/

/*{{{ fileio_write_buffer */

void fileio_write_buffer (FileIO *fop,
    const char *buf, size_t buf_size, off_t off, fuse_ino_t ino,
    FileIO_on_buffer_written_cb on_buffer_written_cb, gpointer ctx)
{

    // XXX: allow only sequentially write
    // current written bytes should be always match offset
    if (fop->current_size != off) {
        LOG_err (FIO_LOG, "Write call with offset %"OFF_FMT" is not allowed !", off);
        on_buffer_written_cb (fop, ctx, FALSE, 0);
        return;
    }

    // CacheMng
    /*
    cache_mng_store_file_buf (application_get_cache_mng (fop->app), 
        ino, buf_size, off, (unsigned char *) buf, 
        NULL, NULL);
    */

    evbuffer_add (fop->write_buf, buf, buf_size);
    
    // data is added to the output buffer
    on_buffer_written_cb (fop, ctx, TRUE, buf_size);
    fop->current_size += buf_size;
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
    
    // release S3HttpConnection
    s3http_connection_release (con);
   
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
        g_printf ("RANGE: %s \n", range_hdr);
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
        LOG_err (FIO_LOG, "Reading from cache");
        rdata->on_buffer_read_cb (rdata->ctx, TRUE, buf, size);
        rdata->fop->tmp_xx = FALSE;
        g_free (rdata);
    } else {
        LOG_err (FIO_LOG, "Reading from server !!");
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

    LOG_err (FIO_LOG, "requesting [%"G_GUINT64_FORMAT" %zu]", rdata->off, rdata->size);

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

    if (rdata->fop->tmp_xx) {
        LOG_err (FIO_LOG, "xxXXxxx");
        exit (1);
    }
    rdata->fop->tmp_xx = TRUE;

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
