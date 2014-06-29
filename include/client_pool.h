/*
 * Copyright (C) 2012-2014 Paul Ionkin <paul.ionkin@gmail.com>
 * Copyright (C) 2012-2014 Skoobe GmbH. All rights reserved.
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
#ifndef _CLIENT_POOL_H_
#define _CLIENT_POOL_H_

#include "global.h"

typedef gpointer (*ClientPool_client_create) (Application *app);
typedef void (*ClientPool_client_destroy) (gpointer client);
typedef void (*ClientPool_on_released_cb) (gpointer client, gpointer ctx);
typedef void (*ClientPool_client_set_on_released_cb) (gpointer client, ClientPool_on_released_cb client_on_released_cb, gpointer ctx);
typedef gboolean (*ClientPool_client_check_rediness) (gpointer client);
typedef void (*ClientPool_client_get_stats_info_data) (gpointer client, GString *str, struct PrintFormat *print_format);
typedef void (*ClientPool_client_get_stats_info_caption) (gpointer client, GString *str, struct PrintFormat *print_format);

ClientPool *client_pool_create (Application *app,
    gint client_count,
    ClientPool_client_create client_create,
    ClientPool_client_destroy client_destroy,
    ClientPool_client_set_on_released_cb client_set_on_released_cb,
    ClientPool_client_check_rediness client_check_rediness,
    ClientPool_client_get_stats_info_caption client_get_stats_info_caption,
    ClientPool_client_get_stats_info_data client_get_stats_info_data
    );

void client_pool_destroy (ClientPool *pool);

// add client's callback to the awaiting queue
// return TRUE if added, FALSE if list is full
typedef void (*ClientPool_on_client_ready) (gpointer client, gpointer ctx);
gboolean client_pool_get_client (ClientPool *pool, ClientPool_on_client_ready on_client_ready, gpointer ctx);
gint client_pool_get_client_count (ClientPool *pool);

typedef void (*ClientPool_on_request_done) (gpointer callback_data, gboolean success);
void client_pool_add_request (ClientPool *pool,
    ClientPool_on_request_done on_request_done, gpointer callback_data);

void client_pool_get_client_stats_info (ClientPool *pool, GString *str, struct PrintFormat *print_format);

#endif
