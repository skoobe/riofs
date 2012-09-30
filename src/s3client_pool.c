#include "include/global.h"
#include "include/s3client_pool.h"

struct _S3ClientPool {
    Application *app;
    struct event_base *evbase;
    struct evdns_base *dns_base;
    GList *l_clients;
    gint max_requests; // maximum awaiting clients in queue
    GQueue *q_requests; // the queue of awaiting client
};

typedef struct {
    S3ClientPool_on_get on_get;
    gpointer ctx;
} RequestData;

#define POOL "pool"

static void s3client_pool_on_client_released (gpointer *client, gpointer ctx);

// creates connection pool object
// create client_count clients
// return NULL if error
S3ClientPool *s3client_pool_create (Application *app, 
    gint client_count,
    S3ClientPool_client_create client_create, 
    S3ClientPool_client_set_on_released_cb client_set_on_released_cb)
{
    S3ClientPool *pool;
    guint i;
    gpointer *client;

    pool = g_new0 (S3ClientPool, 1);
    pool->app = app;
    pool->evbase = application_get_evbase (app);
    pool->dns_base = application_get_dnsbase (app);
    pool->l_clients = NULL;
    pool->q_requests = g_queue_new ();
    pool->max_requests = 100; // XXX: configure it !
   
    for (i = 0; i < client_count; i++) {
        client = client_create (pool->evbase, pool->dns_base);
        // add to the list
        pool->l_clients = g_list_append (pool->l_clients, client);
        // add callback
        client_set_on_released_cb (client, s3_client_pool_on_client_released, pool);
    }

    return pool;
}

void s3client_pool_destroy (S3ClientPool *pool)
{
    g_queue_free_full (pool->q_requests, g_free);
    g_list_free (pool->l_clients);

    g_free (pool);
}

// callback executed when a client done with a request
static void s3client_pool_on_client_released (gpointer client, gpointer ctx)
{
    S3ClientPool *pool = (S3ClientPool *) ctx;
    RequestData *data;

    // if we have a request pending
    data = g_queue_pop_head (pool->q_requests);
    if (data) {
        data->on_client (client, data->ctx);
        g_free (data);
    }
}

// add client's callback to the awaiting queue
// return TRUE if added, FALSE if list is full
gboolean s3client_pool_get_client (S3ClientPool *pool, S3ClientPool_on_client on_client, gpointer ctx)
{
    GList *l;
    RequestData *data;
    
    // check if the awaiting queue is full
    if (g_queue_get_length (pool->q_requests) >= pool->max_requests) {
        LOG_debug (POOL, "Pool's client awaiting queue is full !");
        return FALSE;
    }

    // check if there is a client which is ready to execute a new request
    for (l = g_list_first (pool->l_clients); l; l = g_list_next (l)) {
        // http client is ready, return it to client
        if (s3client_is_ready (http)) {
            on_get (http, ctx);
            return TRUE;
        }
    }

    LOG_debug (POOL, "all Pool's clients are busy ..");
    
    // add client to the end of queue
    data = g_new0 (RequestData, 1);
    data->on_client = on_client;
    data->ctx = ctx;
    g_queue_push_tail (pool->q_requests, data);

    return TRUE;
}
