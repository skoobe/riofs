#ifndef _S3_HTTP_CLIENT_H_
#define _S3_HTTP_CLIENT_H_

#include "include/global.h"

typedef struct _S3HttpClient S3HttpClient;

typedef enum {
    S3Method_get = 0,
    S3Method_put = 1,
} S3HttpClientRequestMethod;

S3HttpClient *s3http_client_create (struct event_base *evbase, struct evdns_base *dns_base);
void s3http_client_destroy (S3HttpClient *http);

void s3http_client_set_output_length (S3HttpClient *http, guint64 output_lenght);
void s3http_client_add_output_header (S3HttpClient *http, const gchar *key, const gchar *value);
void s3http_client_add_output_data (S3HttpClient *http, char *buf, size_t size);

const gchar *s3http_client_get_input_header (S3HttpClient *http, const gchar *key);
gint64 s3http_client_get_input_length (S3HttpClient *http);

// return TRUE if http client is ready to execute a new request
gboolean s3http_client_is_ready (S3HttpClient *http);
// try to connect to the server
gboolean s3http_client_start_request (S3HttpClient *http, S3HttpClientRequestMethod method, const gchar *url);

// set context data for all callback functions
void s3http_client_set_cb_ctx (S3HttpClient *http, gpointer ctx);
// context data for pool callback functions
void s3http_client_set_pool_cb_ctx (S3HttpClient *http, gpointer pool_ctx);

// a chunk of data is received
typedef void (*S3HttpClient_on_input_data_cb) (S3HttpClient *http, struct evbuffer *input_buf, gboolean the_last_part, gpointer ctx);
void s3http_client_set_input_data_cb (S3HttpClient *http, S3HttpClient_on_input_data_cb on_input_data_cb);

// connection is closed
typedef void (*S3HttpClient_on_close_cb) (S3HttpClient *http, gpointer ctx);
void s3http_client_set_close_cb (S3HttpClient *http, S3HttpClient_on_close_cb on_close_cb);

// connection is established
typedef void (*S3HttpClient_on_connection_cb) (S3HttpClient *http, gpointer ctx);
void s3http_client_set_connection_cb (S3HttpClient *http, S3HttpClient_on_connection_cb on_connection_cb);

// request is done
typedef void (*S3HttpClient_on_request_done_pool_cb) (S3HttpClient *http, gpointer pool_ctx);
void s3http_client_set_request_done_pool_cb (S3HttpClient *http, 
    S3HttpClient_on_request_done_pool_cb on_request_done_pool_cb);


#endif
