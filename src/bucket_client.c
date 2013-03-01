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
#include "s3http_connection.h"

typedef struct {
    BucketClient_on_acl_cb on_acl_cb;
    gpointer ctx;
} BucketClient;

#define BCLIENT_LOG "bclient"
static void bucket_client_on_acl_cb (S3HttpConnection *con, gpointer ctx, gboolean success,
    const gchar *buf, size_t buf_len, struct evkeyvalq *headers);

static void bucket_client_get_acl_done (BucketClient *bclient, gboolean success, const gchar *buf, size_t buf_len)
{
    if (bclient->on_acl_cb)
        bclient->on_acl_cb (bclient->ctx, success, buf, buf_len);

    g_free (bclient);
}

void bucket_client_get_acl (S3HttpConnection *con,
    BucketClient_on_acl_cb on_acl_cb, gpointer ctx)
{
    BucketClient *bclient;
    gboolean res;

    bclient = g_new0 (BucketClient, 1);
    bclient->ctx = ctx;
    bclient->on_acl_cb = on_acl_cb;

    res = s3http_connection_make_request (con, 
        "/?acl", "GET", 
        NULL,
        bucket_client_on_acl_cb,
        bclient
    );

    if (!res) {
        LOG_err (BCLIENT_LOG, "Failed to execute HTTP request !");
        bucket_client_get_acl_done (bclient, FALSE, NULL, 0);
    }
}

static void bucket_client_on_acl_cb (S3HttpConnection *con, gpointer ctx, gboolean success,
    G_GNUC_UNUSED const gchar *buf, G_GNUC_UNUSED size_t buf_len, 
    G_GNUC_UNUSED struct evkeyvalq *headers)
{
    BucketClient *bclient = (BucketClient *) ctx;

    s3http_connection_release (con);

    if (!success) {
        LOG_err (BCLIENT_LOG, "Failed to execute HTTP request !");
        bucket_client_get_acl_done (bclient, FALSE, NULL, 0);
        return;
    }
    
    bucket_client_get_acl_done (bclient, TRUE, buf, buf_len);
}
