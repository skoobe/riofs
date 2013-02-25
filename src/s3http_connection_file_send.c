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
#include "s3http_connection.h"
#include "dir_tree.h"

typedef struct {
    S3HttpConnection_on_entry_sent_cb on_entry_sent_cb;
    gpointer ctx;
} FileSendData;

#define CON_SEND_LOG "con_dir"

static void on_file_send_done (S3HttpConnection *con, FileSendData *data, gboolean success)
{
    if (con)
        s3http_connection_release (con);

    if (data && data->on_entry_sent_cb)
        data->on_entry_sent_cb (data->ctx, success);

    g_free (data);
}

static void s3http_connection_on_file_send_cb (S3HttpConnection *con, void *ctx, gboolean success,
        const gchar *buf, size_t buf_len, G_GNUC_UNUSED struct evkeyvalq *headers)
{
    FileSendData *data = (FileSendData *) ctx;

    LOG_debug (CON_SEND_LOG, "File is sent! op: %p", data->ctx);
    
    on_file_send_done (con, data, TRUE);
}

void s3http_connection_file_send (S3HttpConnection *con, int fd, const gchar *resource_path, 
    S3HttpConnection_on_entry_sent_cb on_entry_sent_cb, gpointer ctx)
{
    gboolean res;
    FileSendData *data;
    struct evbuffer *output_buf;
    struct stat st;

    data = g_new0 (FileSendData, 1);
    data->on_entry_sent_cb = on_entry_sent_cb;
    data->ctx = ctx;

    LOG_debug (CON_SEND_LOG, "Sending file.. %p, fd: %d", data->ctx, fd);

    if (fstat (fd, &st) < 0) {
        LOG_err (CON_SEND_LOG, "Failed to stat temp file: %d", fd);
        on_file_send_done (con, data, TRUE);
        return;
    }

    output_buf = evbuffer_new ();
    if (evbuffer_add_file (output_buf, fd, 0, st.st_size) < 0) {
        LOG_err (CON_SEND_LOG, "Failed to read temp file !");
        on_file_send_done (con, data, TRUE);
        return;
    }

    LOG_debug (CON_SEND_LOG, "[%p %p] Sending %s file, req: %"OFF_FMT"  buff: %zd", con, data, 
        resource_path, st.st_size, evbuffer_get_length (output_buf));

    res = s3http_connection_make_request (con, 
        resource_path, "PUT", 
        output_buf,
        s3http_connection_on_file_send_cb,
        data
    );

    evbuffer_free (output_buf);

    if (!res) {
        LOG_err (CON_SEND_LOG, "Failed to create HTTP request !");
        on_file_send_done (con, data, TRUE);
        return;
    }
}
