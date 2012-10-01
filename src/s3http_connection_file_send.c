#include "include/s3http_connection.h"
#include "include/dir_tree.h"

typedef struct {
    S3HttpConnection_on_entry_sent_cb on_entry_sent_cb;
    gpointer ctx;
} FileSendData;

#define CON_SEND_LOG "con_dir"

static void s3http_connection_on_file_send_error (S3HttpConnection *con, void *ctx)
{
    FileSendData *data = (FileSendData *) ctx;

    LOG_err (CON_SEND_LOG, "Failed to send file !");
    
    s3http_connection_release (con);

    if (data->on_entry_sent_cb)
        data->on_entry_sent_cb (ctx, FALSE);

    g_free (data);
}


static void s3http_connection_on_file_send_done (S3HttpConnection *con, void *ctx, const gchar *buf, size_t buf_len)
{
    FileSendData *data = (FileSendData *) ctx;

    LOG_debug (CON_SEND_LOG, "File is sent ! %p %p", data);

    if (data->on_entry_sent_cb)
        data->on_entry_sent_cb (data->ctx, TRUE);
    else
        LOG_msg (CON_SEND_LOG, "No callback function !");

    s3http_connection_release (con);
    
    g_free (data);
}

gboolean s3http_connection_file_send (S3HttpConnection *con, int fd, const gchar *resource_path, 
    S3HttpConnection_on_entry_sent_cb on_entry_sent_cb, gpointer ctx)
{
    gchar *req_path;
    gboolean res;
    FileSendData *data;
    struct evbuffer *output_buf;
    struct stat st;

    data = g_new0 (FileSendData, 1);
    data->on_entry_sent_cb = on_entry_sent_cb;
    data->ctx = ctx;

    LOG_debug (CON_SEND_LOG, "Sending file.. %p", data);

    req_path = g_strdup_printf ("%s", resource_path);

    if (fstat (fd, &st) < 0) {
        LOG_err (CON_SEND_LOG, "Failed to stat temp file !");
        s3http_connection_on_file_send_error (con, (void *) data);
        return FALSE;
    }

    output_buf = evbuffer_new ();
    if (evbuffer_add_file (output_buf, fd, 0, st.st_size) < 0) {
        LOG_err (CON_SEND_LOG, "Failed to read temp file !");
        s3http_connection_on_file_send_error (con, (void *) data);
        return FALSE;
    }

    LOG_debug (CON_SEND_LOG, "[%p %p] Sending %s file, req: %s, %"OFF_FMT"  buff: %zd", con, data, 
        resource_path, req_path, st.st_size, evbuffer_get_length (output_buf));

    res = s3http_connection_make_request (con, 
        resource_path, req_path, "PUT", 
        output_buf,
        s3http_connection_on_file_send_done,
        s3http_connection_on_file_send_error, 
        data
    );

    g_free (req_path);
    evbuffer_free (output_buf);

    if (!res) {
        LOG_err (CON_SEND_LOG, "Failed to create HTTP request !");
        s3http_connection_on_file_send_error (con, (void *) data);
        return FALSE;
    }

    return TRUE;
}
