/*
 * Copyright (C) 2012  Paul Ionkin <paul.ionkin@gmail.com>
 * Copyright (C) 2012 Skoobe GmbH. All rights reserved.
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
#include "global.h"
#include "s3client_pool.h"

struct _S3ClientPool {
    Application *app;
    struct event_base *evbase;
    struct evdns_base *dns_base;
    GList *l_clients; // the list of PoolClient
    guint max_requests; // maximum awaiting clients in queue
    GQueue *q_requests; // the queue of awaiting client
};

typedef struct {
    S3ClientPool *pool;
    S3ClientPool_client_check_rediness client_check_rediness; // is client ready for a new request
    S3ClientPool_client_destroy client_destroy;
    gpointer client;
} PoolClient;

typedef struct {
    S3ClientPool_on_client_ready on_client_ready;
    gpointer ctx;
} RequestData;

#define POOL "pool"

static void s3client_pool_on_client_released (gpointer client, gpointer ctx);

// creates connection pool object
// create client_count clients
// return NULL if error
S3ClientPool *s3client_pool_create (Application *app, 
    gint client_count,
    S3ClientPool_client_create client_create, 
    S3ClientPool_client_destroy client_destroy, 
    S3ClientPool_client_set_on_released_cb client_set_on_released_cb,
    S3ClientPool_client_check_rediness client_check_rediness)
{
    S3ClientPool *pool;
    gint i;
    PoolClient *pc;

    pool = g_new0 (S3ClientPool, 1);
    pool->app = app;
    pool->evbase = application_get_evbase (app);
    pool->dns_base = application_get_dnsbase (app);
    pool->l_clients = NULL;
    pool->q_requests = g_queue_new ();
    pool->max_requests = 100; // XXX: configure it !
   
    for (i = 0; i < client_count; i++) {
        pc = g_new0 (PoolClient, 1);
        pc->pool = pool;
        pc->client = client_create (app);
        pc->client_check_rediness = client_check_rediness;
        pc->client_destroy = client_destroy;
        // add to the list
        pool->l_clients = g_list_append (pool->l_clients, pc);
        // add callback
        client_set_on_released_cb (pc->client, s3client_pool_on_client_released, pc);
    }

    return pool;
}

void s3client_pool_destroy (S3ClientPool *pool)
{
    GList *l;
    PoolClient *pc;

    g_queue_free_full (pool->q_requests, g_free);
    for (l = g_list_first (pool->l_clients); l; l = g_list_next (l)) {
        pc = (PoolClient *) l->data;
        pc->client_destroy (pc->client);
        g_free (pc);
    }
    g_list_free (pool->l_clients);

    g_free (pool);
}

// callback executed when a client done with a request
static void s3client_pool_on_client_released (gpointer client, gpointer ctx)
{
    PoolClient *pc = (PoolClient *) ctx;
    RequestData *data;

    // if we have a request pending
    data = g_queue_pop_head (pc->pool->q_requests);
    if (data) {
        data->on_client_ready (client, data->ctx);
        g_free (data);
    }
}

// add client's callback to the awaiting queue
// return TRUE if added, FALSE if list is full
gboolean s3client_pool_get_client (S3ClientPool *pool, S3ClientPool_on_client_ready on_client_ready, gpointer ctx)
{
    GList *l;
    RequestData *data;
    PoolClient *pc;
    
    // check if the awaiting queue is full
    if (g_queue_get_length (pool->q_requests) >= pool->max_requests) {
        LOG_debug (POOL, "Pool's client awaiting queue is full !");
        return FALSE;
    }

    // check if there is a client which is ready to execute a new request
    for (l = g_list_first (pool->l_clients); l; l = g_list_next (l)) {
        pc = (PoolClient *) l->data;
        
        // check if client is ready
        if (pc->client_check_rediness (pc->client)) {
            on_client_ready (pc->client, ctx);
            return TRUE;
        }
    }

    LOG_debug (POOL, "all Pool's clients are busy ..ctx: %p", ctx);
    
    // add client to the end of queue
    data = g_new0 (RequestData, 1);
    data->on_client_ready = on_client_ready;
    data->ctx = ctx;
    g_queue_push_tail (pool->q_requests, data);

    return TRUE;
}
