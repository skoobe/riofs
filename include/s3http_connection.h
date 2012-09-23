#ifndef _S3_HTTP_CONNECTION_H_
#define _S3_HTTP_CONNECTION_H_

#include "include/global.h"

typedef enum {
    RT_list = 0,
} RequestType;

struct _S3HttpConnection {
    Application *app;

    struct evhttp_connection *evcon;
    gchar *bucket_name;
    struct evhttp_uri *s3_url;
};


S3HttpConnection *s3http_connection_create (Application *app, struct evhttp_uri *s3_url, const gchar *bucket_name);
void s3http_connection_destroy (S3HttpConnection *con);

struct evhttp_connection *s3http_connection_get_evcon (S3HttpConnection *con);
Application *s3http_connection_get_app (S3HttpConnection *con);

gboolean s3http_connection_connect (S3HttpConnection *con);

void s3http_connection_send (S3HttpConnection *con, struct evbuffer *outbuf);


const gchar *s3http_connection_get_auth_string (S3HttpConnection *con, 
        const gchar *method, const gchar *content_type, const gchar *resource);
struct evhttp_request *s3http_connection_create_request (S3HttpConnection *con,
    void (*cb)(struct evhttp_request *, void *), void *arg,
    const gchar *auth_str);


gboolean s3http_connection_get_directory_listing (S3HttpConnection *con, const gchar *path);


typedef void (*S3HttpConnection_get_object_callback) (gpointer callback_data, gboolean success, struct evbuffer *in_data);
gboolean s3http_connection_get_object (S3HttpConnection *con, const gchar *path,
     S3HttpConnection_get_object_callback get_object_callback, gpointer callback_data);

gpointer s3http_connection_put_object_create_req (S3HttpConnection *con, const gchar *path, struct evbuffer *out_buf);

typedef void (*S3HttpConnection_put_object_callback) (gpointer callback_data);
gboolean s3http_connection_put_object (gpointer req_p, struct evbuffer *out_buf,
    S3HttpConnection_put_object_callback put_object_callback, gpointer callback_data);

#endif
