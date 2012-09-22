#ifndef _S3_HTTP_CONNECTION_H_
#define _S3_HTTP_CONNECTION_H_

#include "include/global.h"

typedef enum {
    RT_list = 0,
} RequestType;

struct _S3HTTPConnection {
    Application *app;

    struct evhttp_connection *evcon;
    gchar *bucket_name;
    struct evhttp_uri *s3_url;
};


S3HTTPConnection *s3http_connection_create (Application *app, struct evhttp_uri *s3_url, const gchar *bucket_name);
void s3http_connection_destroy (S3HTTPConnection *con);

struct evhttp_connection *s3http_connection_get_evcon (S3HTTPConnection *con);
Application *s3http_connection_get_app (S3HTTPConnection *con);

gboolean s3http_connection_connect (S3HTTPConnection *con);

void s3http_connection_send (S3HTTPConnection *con, struct evbuffer *outbuf);


const gchar *s3http_connection_get_auth_string (S3HTTPConnection *con, 
        const gchar *method, const gchar *content_type, const gchar *resource);
struct evhttp_request *s3http_connection_create_request (S3HTTPConnection *con,
    void (*cb)(struct evhttp_request *, void *), void *arg,
    const gchar *auth_str);


gboolean s3http_connection_get_directory_listing (S3HTTPConnection *con, const gchar *path);


typedef void (*S3HTTPConnection_get_object_callback) (gpointer callback_data, gboolean success, struct evbuffer *in_data);
gboolean s3http_connection_get_object (S3HTTPConnection *con, const gchar *path,
     S3HTTPConnection_get_object_callback get_object_callback, gpointer callback_data);

gpointer s3http_connection_put_object_create_req (S3HTTPConnection *con, const gchar *path, struct evbuffer *out_buf);

typedef void (*S3HTTPConnection_put_object_callback) (gpointer callback_data);
gboolean s3http_connection_put_object (gpointer req_p, struct evbuffer *out_buf,
    S3HTTPConnection_put_object_callback put_object_callback, gpointer callback_data);

#endif
