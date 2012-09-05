#include "include/bucket_connection.h"
static void bucket_connection_on_object_put (struct evhttp_request *req, void *ctx)
{   
   // GetObectCallbackData *data = (GetObectCallbackData *) ctx;
    struct evbuffer *inbuf;
    size_t buf_len;

    LOG_debug ("Object put callback");
    
    if (!req) {
        LOG_err ("Failed to get response from server !");
        goto done;
    }

    if (evhttp_request_get_response_code (req) != HTTP_OK) {
        LOG_err ("response code: %d", evhttp_request_get_response_code (req));
        goto done;
    }

done:
    return;
}

gboolean bucket_connection_put_object (BucketConnection *con, const gchar *path,
    struct evbuffer *out_buf)
{
    S3Bucket *bucket;
    struct evhttp_request *req;
    gchar *resource_path;
    int res;
    gchar *auth_str;
    struct evbuffer *post_buf;

    LOG_debug ("Putting object: %s", path);

    bucket = bucket_connection_get_bucket (con);

    resource_path = g_strdup_printf ("/%s%s", bucket->name, path);
    auth_str = bucket_connection_get_auth_string (con, "PUT", "", resource_path);
    req = bucket_connection_create_request (con, bucket_connection_on_object_put, NULL, auth_str);
    
    // get the request output buffer
    post_buf = evhttp_request_get_output_buffer (req);
    evbuffer_add_buffer_reference (post_buf, out_buf);

    // send it
    res = evhttp_make_request (bucket_connection_get_evcon (con), req, EVHTTP_REQ_PUT, path);
    
    g_free (auth_str);

    return TRUE;
}
