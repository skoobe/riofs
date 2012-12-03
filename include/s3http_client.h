/*
 * Copyright (C) 2012  Paul Ionkin <paul.ionkin@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */
#ifndef _S3_HTTP_CLIENT_H_
#define _S3_HTTP_CLIENT_H_

#include "global.h"
#include "s3client_pool.h"

typedef struct _S3HttpClient S3HttpClient;

typedef enum {
    S3Method_get = 0,
    S3Method_put = 1,
} S3HttpClientRequestMethod;

gpointer s3http_client_create (Application *app);
void s3http_client_destroy (gpointer data);

void s3http_client_request_reset (S3HttpClient *http);

void s3http_client_set_output_length (S3HttpClient *http, guint64 output_lenght);
void s3http_client_add_output_header (S3HttpClient *http, const gchar *key, const gchar *value);
void s3http_client_add_output_data (S3HttpClient *http, char *buf, size_t size);

const gchar *s3http_client_get_input_header (S3HttpClient *http, const gchar *key);
gint64 s3http_client_get_input_length (S3HttpClient *http);


gboolean s3http_client_check_rediness (gpointer client);
gboolean s3http_client_acquire (gpointer client);
gboolean s3http_client_release (gpointer client);
void s3http_client_set_on_released_cb (gpointer client, S3ClientPool_on_released_cb client_on_released_cb, gpointer ctx);

// return TRUE if http client is ready to execute a new request
gboolean s3http_client_is_ready (S3HttpClient *http);
// try to connect to the server
gboolean s3http_client_start_request (S3HttpClient *http, S3HttpClientRequestMethod method, const gchar *url);

// set context data for all callback functions
void s3http_client_set_cb_ctx (S3HttpClient *http, gpointer ctx);


// a chunk of data is received
typedef void (*S3HttpClient_on_chunk_cb) (S3HttpClient *http, struct evbuffer *input_buf, gpointer ctx);
void s3http_client_set_on_chunk_cb (S3HttpClient *http, S3HttpClient_on_chunk_cb on_chunk_cb);
// last chunk of data is received
void s3http_client_set_on_last_chunk_cb (S3HttpClient *http, S3HttpClient_on_chunk_cb on_last_chunk_cb);



// connection is closed
typedef void (*S3HttpClient_on_close_cb) (S3HttpClient *http, gpointer ctx);
void s3http_client_set_close_cb (S3HttpClient *http, S3HttpClient_on_close_cb on_close_cb);

// connection is established
typedef void (*S3HttpClient_on_connection_cb) (S3HttpClient *http, gpointer ctx);
void s3http_client_set_connection_cb (S3HttpClient *http, S3HttpClient_on_connection_cb on_connection_cb);


#endif
