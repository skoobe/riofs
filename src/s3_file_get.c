#include "include/bucket_connection.h"

typedef struct {
    bucket_connection_get_object_callback get_object_callback;
    gpointer callback_data;
} GetObectCallbackData;

static void bucket_connection_on_object_data (struct evhttp_request *req, void *ctx)
{   
    GetObectCallbackData *data = (GetObectCallbackData *) ctx;
    struct evbuffer *inbuf;
    size_t buf_len;

    LOG_debug ("Object get callback");
    
    if (!req) {
        LOG_err ("Failed to get response from server !");
        data->get_object_callback (data->callback_data, FALSE, NULL);
        goto done;
    }

    if (evhttp_request_get_response_code (req) != HTTP_OK) {
        LOG_err ("response code: %d", evhttp_request_get_response_code (req));
        data->get_object_callback (data->callback_data, FALSE, NULL);
        goto done;
    }

    inbuf = evhttp_request_get_input_buffer (req);

    if (!buf_len) {
        data->get_object_callback (data->callback_data, FALSE, NULL);
        goto done;
    }
   
    data->get_object_callback (data->callback_data, TRUE, inbuf);

done:
    g_free (data);
}

gboolean bucket_connection_get_object (BucketConnection *con, const gchar *path,
    bucket_connection_get_object_callback get_object_callback, gpointer callback_data)
{
    S3Bucket *bucket;
    struct evhttp_request *req;
    gchar *resource_path;
    int res;
    gchar *auth_str;
    GetObectCallbackData *data;

    data = g_new0 (GetObectCallbackData, 1);
    data->get_object_callback = get_object_callback;
    data->callback_data = callback_data;

    LOG_debug ("Getting object: %s", path);

    bucket = bucket_connection_get_bucket (con);

    resource_path = g_strdup_printf ("/%s%s", bucket->name, path);
    auth_str = bucket_connection_get_auth_string (con, "GET", "", resource_path);
    req = bucket_connection_create_request (con, bucket_connection_on_object_data, data, auth_str);
    res = evhttp_make_request (bucket_connection_get_evcon (con), req, EVHTTP_REQ_GET, path);
    
    g_free (auth_str);

    return TRUE;
}
