#include "include/bucket_connection.h"

static void bucket_connection_on_object_data (struct evhttp_request *req, void *ctx)
{   
    struct evbuffer *inbuf;
    const char *buf;
    size_t buf_len;

    if (!req) {
        LOG_err ("Failed to get responce from server !");
        return;
    }

    if (evhttp_request_get_response_code (req) != HTTP_OK) {
        LOG_err ("response code: %d", evhttp_request_get_response_code (req));
        return;
    }

    inbuf = evhttp_request_get_input_buffer (req);
    buf_len = evbuffer_get_length (inbuf);
    buf = (const char *) evbuffer_pullup (inbuf, buf_len);

    if (!buf_len) {
        // XXX: 
        return;
    }
   
    g_printf ("size: %zd\n======================\n%s\n=======================\n", buf_len, buf);

}

gboolean bucket_connection_get_object (BucketConnection *con, const gchar *path)
{
    S3Bucket *bucket;
    struct evhttp_request *req;
    gchar *resource_path;
    int res;
    gchar *auth_str;

    LOG_debug ("Getting object: %s", path);

    bucket = bucket_connection_get_bucket (con);

    resource_path = g_strdup_printf ("/%s%s", bucket->name, path);
    auth_str = bucket_connection_get_auth_string (con, "GET", "", resource_path);
    req = bucket_connection_create_request (con, bucket_connection_on_object_data, NULL, auth_str);
    res = evhttp_make_request (bucket_connection_get_evcon (con), req, EVHTTP_REQ_GET, path);
    
    g_free (auth_str);
}
