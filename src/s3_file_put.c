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

typedef struct {
    bucket_connection_put_object_callback put_object_callback;
    gpointer callback_data;
    struct evbuffer *post_buf;
    struct evbuffer_cb_entry *cb_entry;
} PutObjectCallbackData;

static void on_buffer_depleted_func (struct evbuffer *buffer, const struct evbuffer_cb_info *info, void *arg)
{
    PutObjectCallbackData *data = (PutObjectCallbackData *)arg;

    LOG_debug ("=== BUFFER sent : %zd", info->n_added);
    evbuffer_remove_cb_entry (data->post_buf, data->cb_entry);
    data->put_object_callback (data->callback_data);
    g_free (data);
}


gboolean bucket_connection_put_object (gpointer req_p, struct evbuffer *out_buf,
    bucket_connection_put_object_callback put_object_callback, gpointer callback_data)
{
    struct evhttp_request *req = (struct evhttp_request *)req_p;
    PutObjectCallbackData *data;

    LOG_debug ("Putting object buffer: %zd bytes", evbuffer_get_length (out_buf));

    data = g_new0 (PutObjectCallbackData, 1);
    data->put_object_callback = put_object_callback;
    data->callback_data = callback_data;
    
    // get the request output buffer
    data->post_buf = evhttp_request_get_output_buffer (req);
    evbuffer_add_buffer (data->post_buf, out_buf);
    data->cb_entry = evbuffer_add_cb (data->post_buf, on_buffer_depleted_func, data);

    return TRUE;
}


gpointer bucket_connection_put_object_create_req (BucketConnection *con, const gchar *path,
struct evbuffer *out_buf)
{
    S3Bucket *bucket;
    struct evhttp_request *req;
    gchar *resource_path;
    int res;
    gchar *auth_str;
    struct evbuffer *post_buf;
    struct bufferevent *bev;
    void **cbarg_ptr;

    bufferevent_data_cb *readcb_ptr,
    bufferevent_data_cb *writecb_ptr,
    bufferevent_event_cb *eventcb_ptr,

    LOG_debug ("Creating put handler for object: %s", path);

    bucket = bucket_connection_get_bucket (con);

    resource_path = g_strdup_printf ("/%s%s", bucket->name, path);
    auth_str = bucket_connection_get_auth_string (con, "PUT", "", resource_path);

    req = bucket_connection_create_request (con, bucket_connection_on_object_put, NULL, auth_str);
    
    // get the request output buffer
    post_buf = evhttp_request_get_output_buffer (req);
    evbuffer_add_buffer (post_buf, out_buf);

    bev = evhttp_connection_get_bufferevent (req);
bufferevent_getcb(bev,
     readcb_ptr,
     writecb_ptr,
    eventcb_ptr,
    cbarg_ptr);

bufferevent_setcb (bev, NULL, 
    
    // send it
    res = evhttp_make_request (bucket_connection_get_evcon (con), req, EVHTTP_REQ_PUT, path);
    
    g_free (auth_str);

    return req;
}

