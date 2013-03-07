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
#ifndef _HTTP_CONNECTION_H_
#define _HTTP_CONNECTION_H_

#include "global.h"
#include "client_pool.h"

typedef enum {
    RT_list = 0,
} RequestType;

struct _HttpConnection {
    Application *app;
    ConfData *conf;

    ClientPool_on_released_cb client_on_released_cb;
    gpointer pool_ctx;

    struct evhttp_connection *evcon;

    // is taken by high level
    gboolean is_acquired;
    GList *l_output_headers;
};

gpointer http_connection_create (Application *app);
void http_connection_destroy (gpointer data);

void http_connection_add_output_header (HttpConnection *con, const gchar *key, const gchar *value);

void http_connection_set_on_released_cb (gpointer client, ClientPool_on_released_cb client_on_released_cb, gpointer ctx);
gboolean http_connection_check_rediness (gpointer client);
gboolean http_connection_acquire (HttpConnection *con);
gboolean http_connection_release (HttpConnection *con);

struct evhttp_connection *http_connection_get_evcon (HttpConnection *con);
Application *http_connection_get_app (HttpConnection *con);

gboolean http_connection_connect (HttpConnection *con);

void http_connection_send (HttpConnection *con, struct evbuffer *outbuf);

typedef void (*HttpConnection_directory_listing_callback) (gpointer callback_data, gboolean success);
void http_connection_get_directory_listing (HttpConnection *con, const gchar *path, fuse_ino_t ino,
    HttpConnection_directory_listing_callback directory_listing_callback, gpointer callback_data);


typedef void (*BucketClient_on_acl_cb) (gpointer callback_data, gboolean success, const gchar *buf, size_t buf_len);
void bucket_client_get_acl (HttpConnection *con, BucketClient_on_acl_cb on_acl_cb, gpointer ctx);

typedef void (*HttpConnection_on_entry_sent_cb) (gpointer ctx, gboolean success);
void http_connection_file_send (HttpConnection *con, int fd, const gchar *resource_path, 
    HttpConnection_on_entry_sent_cb on_entry_sent_cb, gpointer ctx);


typedef void (*HttpConnection_responce_cb) (HttpConnection *con, gpointer ctx, gboolean success,
        const gchar *buf, size_t buf_len, struct evkeyvalq *headers);
gboolean http_connection_make_request (HttpConnection *con, 
    const gchar *resource_path,
    const gchar *http_cmd,
    struct evbuffer *out_buffer,
    HttpConnection_responce_cb responce_cb,
    gpointer ctx);

#endif
