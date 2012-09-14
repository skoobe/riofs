#ifndef _S3_HTTP_CLIENT_H_
#define _S3_HTTP_CLIENT_H_

#include "global.h"

typedef struct _S3Connection S3Connection;
typedef struct _S3Request S3Request;

typedef enum {
    S3Method_get = 0,
    S3Method_put = 1,
} S3RequestMethod;

S3Connection *s3connection_new (struct event_base *evbase, struct evdns_base *dns_base, S3RequestMethod method, const gchar *url);
void s3connection_destroy (S3Connection *con);

void s3connection_add_output_header (S3Connection *con, const gchar *key, const gchar *value);
void s3connection_add_output_data (S3Connection *con, char *buf, size_t size);

const gchar *s3connection_get_input_header (S3Connection *con, const gchar *key);
gint64 s3connection_get_input_length (S3Connection *con);

gboolean s3connection_start_request (S3Connection *con);



void s3connection_set_cb_data (S3Connection *con, gpointer ctx);

typedef void (*S3Connection_on_input_data_cb) (S3Connection *con, struct evbuffer *input_buf, gpointer ctx);
void s3connection_set_input_data_cb (S3Connection *con,  S3Connection_on_input_data_cb on_input_data_cb);


typedef void (*S3Connection_on_close_cb) (S3Connection *con, gpointer ctx);
void s3connection_set_close_cb (S3Connection *con,  S3Connection_on_close_cb on_close_cb);

typedef void (*S3Connection_on_connection_cb) (S3Connection *con, gpointer ctx);
void s3connection_set_close_cb (S3Connection *con,  S3Connection_on_close_cb on_close_cb);

#endif
