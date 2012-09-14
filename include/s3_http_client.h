#ifndef _S3_HTTP_CLIENT_H_
#define _S3_HTTP_CLIENT_H_

#include "global.h"

typedef struct _S3Http S3Http;

typedef enum {
    S3Method_get = 0,
    S3Method_put = 1,
} S3HttpRequestMethod;

S3Http *s3http_new (struct event_base *evbase, struct evdns_base *dns_base, S3RequestMethod method, const gchar *url);
void s3http_destroy (S3Http *http);

void s3http_add_output_header (S3Http *http, const gchar *key, const gchar *value);
void s3http_add_output_data (S3Http *http, char *buf, size_t size);

const gchar *s3http_get_input_header (S3Http *http, const gchar *key);
gint64 s3http_get_input_length (S3Http *http);

// try to connect to the server
gboolean s3http_start_request (S3Http *http);

// set context data for all callback functions
void s3http_set_cb_data (S3Http *http, gpointer ctx);

// a chunk of data is received
typedef void (*s3http_on_input_data_cb) (S3Http *http, struct evbuffer *input_buf, gpointer ctx);
void s3http_set_input_data_cb (S3Http *http,  s3http_on_input_data_cb on_input_data_cb);

// connection is closed
typedef void (*s3http_on_close_cb) (S3Http *http, gpointer ctx);
void s3http_set_close_cb (S3Http *http,  s3http_on_close_cb on_close_cb);

// connection is established
typedef void (*s3http_on_connection_cb) (S3Http *http, gpointer ctx);
void s3http_set_close_cb (S3Http *http,  s3http_on_close_cb on_close_cb);

#endif
