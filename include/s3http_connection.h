#ifndef _S3_HTTP_CONNECTION_H_
#define _S3_HTTP_CONNECTION_H_

#include "include/global.h"
#include "include/s3client_pool.h"

typedef enum {
    RT_list = 0,
} RequestType;

struct _S3HttpConnection {
    Application *app;

    S3ClientPool_on_released_cb client_on_released_cb;
    gpointer pool_ctx;

    struct evhttp_connection *evcon;
    gchar *bucket_name;
    struct evhttp_uri *s3_uri;
};


S3HttpConnection *s3http_connection_create (Application *app);
void s3http_connection_destroy (S3HttpConnection *con);

void s3http_connection_set_on_released_cb (S3HttpConnection *con, S3ClientPool_on_released_cb client_on_released_cb, gpointer ctx);

struct evhttp_connection *s3http_connection_get_evcon (S3HttpConnection *con);
Application *s3http_connection_get_app (S3HttpConnection *con);

gboolean s3http_connection_connect (S3HttpConnection *con);

void s3http_connection_send (S3HttpConnection *con, struct evbuffer *outbuf);


struct evhttp_request *s3http_connection_create_request (S3HttpConnection *con,
    void (*cb)(struct evhttp_request *, void *), void *arg,
    const gchar *auth_str);


typedef void (*S3HttpConnection_directory_listing_callback) (gpointer callback_data, gboolean success);
gboolean s3http_connection_get_directory_listing (S3HttpConnection *con, const gchar *path, fuse_ino_t ino,
    S3HttpConnection_directory_listing_callback directory_listing_callback, gpointer callback_data);


gboolean s3http_connection_file_send (S3HttpConnection *con, int fd, const gchar *resource_path);


typedef void (*S3HttpConnection_responce_cb) (gpointer ctx, const gchar *buf, size_t buf_len);
typedef void (*S3HttpConnection_error_cb) (gpointer ctx);

gboolean s3http_connection_make_request (S3HttpConnection *con, 
    const gchar *resource_path, const gchar *request_str,
    const gchar *http_cmd,
    struct evbuffer *out_buffer,
    S3HttpConnection_responce_cb responce_cb,
    S3HttpConnection_error_cb error_cb,
    gpointer ctx);

#endif
