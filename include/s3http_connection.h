/*
 * Copyright (C) 2012-2013 Paul Ionkin <paul.ionkin@gmail.com>
 * Copyright (C) 2012-2013 Skoobe GmbH. All rights reserved.
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
#ifndef _S3_HTTP_CONNECTION_H_
#define _S3_HTTP_CONNECTION_H_

#include "global.h"
#include "s3client_pool.h"

typedef enum {
    RT_list = 0,
} RequestType;

struct _S3HttpConnection {
    Application *app;
    ConfData *conf;

    S3ClientPool_on_released_cb client_on_released_cb;
    gpointer pool_ctx;

    struct evhttp_connection *evcon;

    // is taken by high level
    gboolean is_acquired;
};

gpointer s3http_connection_create (Application *app);
void s3http_connection_destroy (gpointer data);

gchar *s3http_connection_get_auth_string (Application *app, 
        const gchar *method, const gchar *content_type, const gchar *resource, const gchar *time_str);

void s3http_connection_set_on_released_cb (gpointer client, S3ClientPool_on_released_cb client_on_released_cb, gpointer ctx);
gboolean s3http_connection_check_rediness (gpointer client);
gboolean s3http_connection_acquire (S3HttpConnection *con);
gboolean s3http_connection_release (S3HttpConnection *con);

struct evhttp_connection *s3http_connection_get_evcon (S3HttpConnection *con);
Application *s3http_connection_get_app (S3HttpConnection *con);

gboolean s3http_connection_connect (S3HttpConnection *con);

void s3http_connection_send (S3HttpConnection *con, struct evbuffer *outbuf);

struct evhttp_request *s3http_connection_create_request (S3HttpConnection *con,
    void (*cb)(struct evhttp_request *, void *), void *arg,
    const gchar *auth_str);


typedef void (*S3HttpConnection_directory_listing_callback) (gpointer callback_data, gboolean success);
void s3http_connection_get_directory_listing (S3HttpConnection *con, const gchar *path, fuse_ino_t ino,
    S3HttpConnection_directory_listing_callback directory_listing_callback, gpointer callback_data);


typedef void (*BucketClient_on_acl_cb) (gpointer callback_data, gboolean success, const gchar *buf, size_t buf_len);
void bucket_client_get_acl (S3HttpConnection *con, BucketClient_on_acl_cb on_acl_cb, gpointer ctx);

typedef void (*S3HttpConnection_on_entry_sent_cb) (gpointer ctx, gboolean success);
void s3http_connection_file_send (S3HttpConnection *con, int fd, const gchar *resource_path, 
    S3HttpConnection_on_entry_sent_cb on_entry_sent_cb, gpointer ctx);


typedef void (*S3HttpConnection_responce_cb) (S3HttpConnection *con, gpointer ctx, gboolean success,
        const gchar *buf, size_t buf_len, struct evkeyvalq *headers);
gboolean s3http_connection_make_request (S3HttpConnection *con, 
    const gchar *resource_path,
    const gchar *http_cmd,
    struct evbuffer *out_buffer,
    S3HttpConnection_responce_cb responce_cb,
    gpointer ctx);

#endif
