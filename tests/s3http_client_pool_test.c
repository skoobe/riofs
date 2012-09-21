#include "include/global.h"
#include "include/s3http_client.h"
#include "include/s3http_client_pool.h"

static void on_get_http_client (S3HttpClient *http, gpointer pool_ctx)
{
    const gchar uri[] = "http://127.0.0.1:8080/test1";
    LOG_debug ("Got http client");
    s3http_client_set_output_length (http, 1);
    s3http_client_start_request (http, S3Method_get, uri);
}

static void on_output_timer (evutil_socket_t fd, short event, void *ctx)
{
    gint i;
    S3HttpClientPool *pool = (S3HttpClientPool *)ctx;
    for (i = 0; i < 10; i++)
        g_assert (s3http_client_pool_get_S3HttpClient (pool, on_get_http_client, NULL) != NULL);
}


static void on_srv_request (struct evhttp_request *req, void *ctx)
{
    struct evbuffer *in;

    in = evhttp_request_get_input_buffer (req);
    LOG_debug ("SRV: received %d bytes", evbuffer_get_length (in));
}

static void start_srv (struct event_base *base)
{
    struct evhttp *http;

    http = evhttp_new (base);
    evhttp_bind_socket (http, "127.0.0.1", 8080);
    evhttp_set_gencb (http, on_srv_request, NULL);
}

int main (int argc, char *argv[])
{
    struct event_base *evbase;
    struct evdns_base *dns_base;
    S3HttpClientPool *pool;
    struct event *timeout;
    struct timeval tv;

    event_set_mem_functions (g_malloc, g_realloc, g_free);

    evbase = event_base_new ();
	dns_base = evdns_base_new (evbase, 1);
    // start server
    start_srv (evbase);

    pool = s3http_client_pool_create (evbase, dns_base, 10);

    timeout = evtimer_new (evbase, on_output_timer, pool);

    evutil_timerclear(&tv);
    tv.tv_sec = 0;
    tv.tv_usec = 500;
    event_add(timeout, &tv);

    event_base_dispatch (evbase);

    return 0;
}
