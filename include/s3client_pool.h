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
#ifndef _S3_CLIENT_POOL_H_
#define _S3_CLIENT_POOL_H_

#include "global.h"

typedef gpointer (*S3ClientPool_client_create) (Application *app);
typedef void (*S3ClientPool_client_destroy) (gpointer client);
typedef void (*S3ClientPool_on_released_cb) (gpointer client, gpointer ctx);
typedef void (*S3ClientPool_client_set_on_released_cb) (gpointer client, S3ClientPool_on_released_cb client_on_released_cb, gpointer ctx);
typedef gboolean (*S3ClientPool_client_check_rediness) (gpointer client);

S3ClientPool *s3client_pool_create (Application *app, 
    gint client_count,
    S3ClientPool_client_create client_create,
    S3ClientPool_client_destroy client_destroy,
    S3ClientPool_client_set_on_released_cb client_set_on_released_cb,
    S3ClientPool_client_check_rediness client_check_rediness);

void s3client_pool_destroy (S3ClientPool *pool);

// add client's callback to the awaiting queue
// return TRUE if added, FALSE if list is full
typedef void (*S3ClientPool_on_client_ready) (gpointer client, gpointer ctx);
gboolean s3client_pool_get_client (S3ClientPool *pool, S3ClientPool_on_client_ready on_client_ready, gpointer ctx);

typedef void (*S3ClientPool_on_request_done) (gpointer callback_data, gboolean success);
void s3client_pool_add_request (S3ClientPool *pool, 
    S3ClientPool_on_request_done on_request_done, gpointer callback_data);
#endif
