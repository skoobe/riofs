#include "include/s3_connection_pool.h"

struct _S3HttpClientPool {
    Application *app;
    GList *l_http_clients;
    GList *l_requests; // the list of awaiting client
};

// creates connection pool object
// reutnr NULL if error
S3HttpClientPool *s3_http_client_pool_new (Application *app)
{
    S3HttpClientPool *pool;

    pool = g_new0 (S3HttpClientPool, 1);
    pool->app = app;
    pool->l_connections = NULL;
    pool->l_requests = NULL;

    return pool;
}

void s3_http_client_pool_destroy (S3HttpClientPool *pool)
{
    g_free (pool);
}

gboolean s3_http_client_pool_connect (S3HttpClientPool *pool, guint count)
{
    guint i;
    S3HttpClient *http;

    for (i = 0; i < count; i++) {
        http = s3_http_client_create (
            application_get_evbase (pool->app),
            application_get_dnsbase (pool->app)
        );
    }
}

// add client's callback to the awaiting list
// return TRUE if added, FALSE if list is full
gboolean s3_http_client_pool_get (S3HttpClientPool *pool, S3HttpClientPool_on_get on_get)
{
}
