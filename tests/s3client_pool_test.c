#include "include/global.h"
#include "include/s3http_client.h"
#include "include/s3http_connection.h"
#include "include/s3client_pool.h"

#define POOL_TEST "pool_test"

static void on_get_http_client (S3HttpClient *http, gpointer pool_ctx)
{
    const gchar uri[] = "http://127.0.0.1:8080/test1";
    LOG_debug (POOL_TEST, "Got http client");
    s3http_client_set_output_length (http, 1);
    s3http_client_start_request (http, S3Method_get, uri);
}

static void on_output_timer (evutil_socket_t fd, short event, void *ctx)
{
    gint i;
    S3ClientPool *pool = (S3ClientPool *)ctx;
    for (i = 0; i < 10; i++)
        g_assert (s3client_pool_get_client (pool, on_get_http_client, NULL) != NULL);
}


static void on_srv_request (struct evhttp_request *req, void *ctx)
{
    struct evbuffer *in;

    in = evhttp_request_get_input_buffer (req);
    LOG_debug (POOL_TEST, "SRV: received %d bytes", evbuffer_get_length (in));
}

static void start_srv (struct event_base *base)
{
    struct evhttp *http;

    http = evhttp_new (base);
    evhttp_bind_socket (http, "127.0.0.1", 8080);
    evhttp_set_gencb (http, on_srv_request, NULL);
}

struct _Application {
    struct event_base *evbase;
    struct evdns_base *dns_base;
};

struct event_base *application_get_evbase (Application *app)
{
    return app->evbase;
}

struct evdns_base *application_get_dnsbase (Application *app)
{
    return app->dns_base;
}

int main (int argc, char *argv[])
{
    S3ClientPool *pool;
    struct event *timeout;
    struct timeval tv;
    Application *app;

    event_set_mem_functions (g_malloc, g_realloc, g_free);

    app = g_new0 (Application, 1);
    app->evbase = event_base_new ();
	app->dns_base = evdns_base_new (app->evbase, 1);
    // start server
    start_srv (app->evbase);
/*
    pool = s3client_pool_create (app, 10,
        s3http_connection_create,
        s3http_connection_set_on_released_cb,
        s3http_connection_check_rediness
    );
*/
    timeout = evtimer_new (app->evbase, on_output_timer, pool);

    evutil_timerclear(&tv);
    tv.tv_sec = 0;
    tv.tv_usec = 500;
    event_add (timeout, &tv);

    event_base_dispatch (app->evbase);

    return 0;
}
