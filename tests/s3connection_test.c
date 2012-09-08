#include "global.h"
#include "s3_http_client.h"

static void on_connection (struct evhttp_request *req, void *h)
{
    struct evbuffer *in_buf;

    in_buf = req->input_buffer;
    printf ("HTTP connection:  got : %zd bytes ! \n", evbuffer_get_length (in_buf));
}


void on_input_data_cb (S3Connection *con, struct evbuffer *input_buf, gpointer ctx)
{
    LOG_debug (">>>> got %zd bytes! Total: %ld length", 
        evbuffer_get_length (input_buf), s3connection_get_input_length (con));
}

int main (int argc, char *argv[])
{
    struct event_base *evbase;
    struct evdns_base *dns_base;
    gboolean srv = FALSE;
    S3Connection *con;
    S3Request *req;
    struct evhttp *http;
    char test[] = "Hello !!!";

    event_set_mem_functions (g_malloc, g_realloc, g_free);

    evbase = event_base_new ();
	dns_base = evdns_base_new (evbase, 0);

    if (argc > 1 && !strcmp (argv[1], "srv")) {
        srv = TRUE;
        http = evhttp_new (evbase);
        evhttp_bind_socket (http, "127.0.0.1", 8080);
        evhttp_set_gencb (http, on_connection, http);
    } else {

        con = s3connection_new (evbase, dns_base, S3RM_get, "http://127.0.0.1:80/test?aaa=bbb&cc=123");

        s3connection_add_output_header (con, "Test", "aaaa");
        s3connection_add_output_data (con, test, sizeof (test));

        s3connection_set_input_data_cb (con, on_input_data_cb, NULL);

        s3connection_start_request (con);
    }
    
    event_base_dispatch (evbase);
    
    if (srv)
        evhttp_free (http);
    else
        s3connection_destroy (con);

    event_base_free (evbase);
    evdns_base_free (dns_base, 0);

    return 0;
}
