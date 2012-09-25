#include "include/global.h"
#include "include/s3http_client_pool.h"

struct _S3HttpClientPool {
    struct event_base *evbase;
    struct evdns_base *dns_base;
    GList *l_http_clients;
    gint max_requests; // maximum awaiting clients in queue
    GQueue *q_requests; // the queue of awaiting client
};

typedef struct {
    S3HttpClientPool_on_get on_get;
    gpointer ctx;
} RequestData;

#define HTTP_POOL "http_pool"

static void s3http_client_pool_on_request_done (S3HttpClient *http, gpointer ctx);

// creates connection pool object
// create client_count HTTP clients
// return NULL if error
S3HttpClientPool *s3http_client_pool_create (struct event_base *evbase, struct evdns_base *dns_base, gint client_count)
{
    S3HttpClientPool *pool;
    guint i;
    S3HttpClient *http;

    pool = g_new0 (S3HttpClientPool, 1);
    pool->evbase = evbase;
    pool->dns_base = dns_base;
    pool->l_http_clients = NULL;
    pool->q_requests = g_queue_new ();
    pool->max_requests = 100; // XXX: configure it !
   
    for (i = 0; i < client_count; i++) {
        http = s3http_client_create (pool->evbase, pool->dns_base);
        // add to the list
        pool->l_http_clients = g_list_append (pool->l_http_clients, http);
        // add callback
        s3http_client_set_pool_cb_ctx (http, pool);
        s3http_client_set_request_done_pool_cb (http, s3http_client_pool_on_request_done);
    }

    return pool;
}

void s3http_client_pool_destroy (S3HttpClientPool *pool)
{
    g_queue_free_full (pool->q_requests, g_free);
    g_free (pool);
}

// callback executed when a S3HttpClient done with a request
static void s3http_client_pool_on_request_done (S3HttpClient *http, gpointer ctx)
{
    S3HttpClientPool *pool = (S3HttpClientPool *) ctx;
    RequestData *data;

    // if we have a client waiting
    data = g_queue_pop_head (pool->q_requests);
    if (data) {
        data->on_get (http, data->ctx);
        g_free (data);
    }
}

// add client's callback to the awaiting queue
// return TRUE if added, FALSE if list is full
gboolean s3http_client_pool_get_S3HttpClient (S3HttpClientPool *pool, S3HttpClientPool_on_get on_get, gpointer ctx)
{
    GList *l;
    RequestData *data;
    
    // check if the awaiting queue is full
    if (g_queue_get_length (pool->q_requests) >= pool->max_requests) {
        LOG_err (HTTP_POOL, "HTTP client awaiting queue is full !");
        return FALSE;
    }

    // check if there is a http client which is ready to execute a new request
    for (l = g_list_first (pool->l_http_clients); l; l = g_list_next (l)) {
        S3HttpClient *http = (S3HttpClient *) l->data;

        // http client is ready, return it to client
        if (s3http_client_is_ready (http)) {
            on_get (http, ctx);
            return TRUE;
        }
    }

    LOG_debug (HTTP_POOL, "all HTTP clients are busy ..");
    
    // add client to the end of queue
    data = g_new0 (RequestData, 1);
    data->on_get = on_get;
    data->ctx = ctx;
    g_queue_push_tail (pool->q_requests, data);

    return TRUE;
}
