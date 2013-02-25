/*
 * Copyright (C) 2012 Paul Ionkin <paul.ionkin@gmail.com>
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
#include "s3http_client.h"

typedef enum {
    TID_first_line_not_sent,
    TID_first_line_sent,
    TID_header_not_sent,
    TID_header_sent,
    TID_header_sent_ok,
    TID_header_sent_two,
    TID_header_sent_two_ok,
    TID_body,
    TID_last_test
} TestID;

typedef struct {
    TestID test_id;
    gint req_count;
    size_t timer_count;
    gboolean header_sent;

    struct event *timeout;
    struct evbuffer *out_buf;
    struct bufferevent *bev;

    gchar *first_line;
    gchar *header_line;
	struct evconnlistener *listener;
    struct event_base *evbase;
    size_t first_line_size;
    size_t header_line_size;

    struct evbuffer *in_file;
    off_t in_file_size;

    struct evhttp *evhttp;
    S3HttpClient *http;
} OutData;

struct _Application {
    struct event_base *evbase;
    struct evdns_base *dns_base;
    ConfData *conf;
};

#define HTTP_TEST "http_test"

struct event_base *application_get_evbase (Application *app)
{
    return app->evbase;
}

struct evdns_base *application_get_dnsbase (Application *app)
{
    return app->dns_base;
}

ConfData *application_get_conf (Application *app)
{
    return app->conf;
}

static void on_output_timer (evutil_socket_t fd, short event, void *ctx)
{
    OutData *out = (OutData *) ctx;
    struct timeval tv;
    struct evbuffer *out_buf;
    char *buf;
    char c;

    LOG_debug (HTTP_TEST, "SRV: on output timer ..");

    if (out->test_id < TID_body && out->timer_count >= evbuffer_get_length (out->out_buf)) {
        bufferevent_free (out->bev);
        evconnlistener_disable (out->listener);
        event_base_loopbreak (out->evbase);
        LOG_debug (HTTP_TEST, "SRV: All headers data sent !! ");
        return;
    }
    
    out_buf = evbuffer_new ();

    if (out->test_id < TID_body) {
        buf = (char *)evbuffer_pullup (out->out_buf, -1);
        c = buf[out->timer_count];

        evbuffer_add (out_buf, &c, sizeof (c));
        out->timer_count++;
        LOG_debug (HTTP_TEST, "SRV: Sending %zd bytes:\n>>%s<<\n", evbuffer_get_length (out_buf), evbuffer_pullup (out_buf, -1));
    } else {
        if (!out->header_sent) {
            evbuffer_add_buffer (out_buf, out->out_buf);
            out->header_sent = TRUE;
        }
        /*
        if (evbuffer_get_length (out->in_file) < 1) {
            bufferevent_free (out->bev);
            evconnlistener_disable (out->listener);
            event_base_loopbreak (out->evbase);
            LOG_debug (HTTP_TEST, "SRV: All data sent !! ");
            return;
        }*/
        evbuffer_remove_buffer (out->in_file, out_buf, 1024*100);

        LOG_debug (HTTP_TEST, "SRV: Sending BODY %zd bytes", evbuffer_get_length (out_buf));
    }

    bufferevent_write_buffer (out->bev, out_buf);
    evbuffer_free (out_buf);
    
    evutil_timerclear(&tv);
    tv.tv_sec = 0;
    tv.tv_usec = 500;
    event_add(out->timeout, &tv);
}

static void srv_read_cb (struct bufferevent *bev, void *ctx)
{
    OutData *out = (OutData *) ctx;
    struct timeval tv;

    LOG_debug (HTTP_TEST, "SRV: on reading ..");

    out->req_count++;

	evutil_timerclear(&tv);
	tv.tv_sec = 0;
	tv.tv_usec = 500;
	event_add(out->timeout, &tv);
}

static void srv_event_cb (struct bufferevent *bev, short what, void *ctx)
{
    LOG_debug (HTTP_TEST, "SRV: on event ..");
}

static void accept_cb (G_GNUC_UNUSED struct evconnlistener *listener, evutil_socket_t fd,
    G_GNUC_UNUSED struct sockaddr *a, G_GNUC_UNUSED int slen, void *p)
{
    OutData *out = (OutData *) p;
    const char first_line[] = "HTTP/1.1 200 OKokokookok\n";
    const char header_line[] = "Content-Length: %lld\n";
    const char header_line_ok[] = "Content-Length: %lld\n\n";
    const char header_line_2[] = "Content-Length: %lld\nServer: Testt asd aaa\n";
    const char header_line_2_ok[] = "Content-Length: %lld\nServer: Testt asd aaa\n\n";

    out->bev = bufferevent_socket_new (out->evbase, fd, BEV_OPT_CLOSE_ON_FREE|BEV_OPT_DEFER_CALLBACKS);
    out->req_count = 0;
    out->timeout = evtimer_new (out->evbase, on_output_timer, out);
    out->out_buf = evbuffer_new ();
    
    if (out->test_id == TID_first_line_not_sent) {
        out->first_line_size = 16;
        out->first_line = g_strdup (first_line);
    } else if (out->test_id == TID_first_line_sent) {
        out->first_line = g_strdup (first_line);
        out->first_line_size = strlen (first_line);
    } else if (out->test_id == TID_header_not_sent) {
        out->first_line = g_strdup (first_line);
        out->first_line_size = strlen (first_line) ;

        out->header_line_size = 17;
        out->header_line = g_strdup_printf (header_line, out->in_file_size);
    } else if (out->test_id == TID_header_sent) {
        out->first_line_size = strlen (first_line) ;
        out->first_line = g_strdup (first_line);

        out->header_line = g_strdup_printf (header_line, out->in_file_size);
        out->header_line_size = strlen (out->header_line);
    } else if (out->test_id == TID_header_sent_ok) {
        out->first_line_size = strlen (first_line) ;
        out->first_line = g_strdup (first_line);

        out->header_line = g_strdup_printf (header_line_ok, out->in_file_size);
        out->header_line_size = strlen (out->header_line);
    } else if (out->test_id == TID_header_sent_two) {
        out->first_line_size = strlen (first_line) ;
        out->first_line = g_strdup (first_line);

        out->header_line = g_strdup_printf (header_line_2, out->in_file_size);
        out->header_line_size = strlen (out->header_line);
    } else if (out->test_id == TID_header_sent_two_ok) {
        out->first_line_size = strlen (first_line) ;
        out->first_line = g_strdup (first_line);

        out->header_line = g_strdup_printf (header_line_2_ok, out->in_file_size);
        out->header_line_size = strlen (out->header_line);
    } else if (out->test_id == TID_body) {
        out->first_line_size = strlen (first_line) ;
        out->first_line = g_strdup (first_line);

        out->header_line = g_strdup_printf (header_line_2_ok, out->in_file_size);
        out->header_line_size = strlen (out->header_line);
        out->header_sent = FALSE;
    }
    
    evbuffer_add (out->out_buf, out->first_line, out->first_line_size);
    if (out->header_line_size)
        evbuffer_add (out->out_buf, out->header_line, out->header_line_size);

    LOG_debug (HTTP_TEST, "SRV: New client connected ! %zd == %zd\nBUF: >>%s<<", sizeof (first_line), strlen (first_line),
        evbuffer_pullup (out->out_buf, -1));
    bufferevent_setcb (out->bev, 
        srv_read_cb, NULL, srv_event_cb,
        out
    );

	bufferevent_enable(out->bev, EV_READ|EV_WRITE);
}

static void start_srv (OutData *out)
{
	struct sockaddr_in sin;
    int port = 8080;
    struct stat st;
    int fd;
    char in_file[] = "./file.in";
    
    // read input file
	if (stat (in_file, &st) == -1) {
        LOG_err (HTTP_TEST, "Failed to stat file %s", in_file);
        return;
    }
    out->in_file_size = st.st_size;

    fd = open (in_file, O_RDONLY);
    if (fd < 0) {
        LOG_err (HTTP_TEST, "Failed to open file %s", in_file);
        return;
    }

    out->in_file = evbuffer_new ();
    
    evbuffer_add_file (out->in_file, fd, 0, st.st_size);
    LOG_debug (HTTP_TEST, "SRV: filesize %ld bytes, in buf: %zd", out->in_file_size, evbuffer_get_length (out->in_file));

    // start listening on port
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_port = htons (port);
    
    out->listener = evconnlistener_new_bind (out->evbase, accept_cb, out, 
        LEV_OPT_CLOSE_ON_FREE|LEV_OPT_CLOSE_ON_EXEC|LEV_OPT_REUSEABLE,
        -1, (struct sockaddr*)&sin, sizeof (sin)
    );
    if (!out->listener)
        LOG_err (HTTP_TEST, "OPS !");
}


void on_input_data_cb (S3HttpClient *http, struct evbuffer *input_buf, gpointer ctx)
{
    struct evbuffer *in_buf = (struct evbuffer *) ctx;
    LOG_debug (HTTP_TEST, "CLN:  >>>> got %zd bytes! Total: %ld length.", 
        evbuffer_get_length (input_buf), s3http_client_get_input_length (http));
    evbuffer_add_buffer (in_buf, input_buf);
    LOG_debug (HTTP_TEST, "CLN: Resulting buf: %zd", evbuffer_get_length (in_buf));
}

static void run_responce_test (struct event_base *evbase, struct evdns_base *dns_base, TestID test_id)
{
    OutData *out;
    struct evbuffer *in_buf;
    S3HttpClient *http;
    Application *app;

    LOG_debug (HTTP_TEST, "===================== TEST ID : %d  =======================", test_id);
    out = g_new0 (OutData, 1);
    out->evbase = evbase;
    out->test_id = test_id;
    out->header_sent = FALSE;

    start_srv (out);
    
    //http = s3http_client_create (app);
    in_buf = evbuffer_new ();

    s3http_client_set_cb_ctx (http, in_buf);
    s3http_client_set_on_chunk_cb (http, on_input_data_cb);
    s3http_client_set_output_length (http, 1);

    s3http_client_start_request (http, S3Method_get, "http://127.0.0.1:8080/index.html");
    
    event_base_dispatch (evbase);
    
    s3http_client_destroy (http);

    LOG_debug (HTTP_TEST, "Resulting buff: %zd", evbuffer_get_length (in_buf));
    evbuffer_free (in_buf);

    g_free (out->first_line);
    g_free (out->header_line);
    evconnlistener_free (out->listener);
    evtimer_del (out->timeout);
    event_free (out->timeout);
    evbuffer_free (out->out_buf);
    evbuffer_free (out->in_file);

    g_free (out);
    LOG_debug (HTTP_TEST, "===================== END TEST ID : %d  =======================", test_id);
}
static void on_request_gencb (struct evhttp_request *req, void *ctx)
{
    OutData *out = (OutData *)ctx;
    LOG_debug (HTTP_TEST, "SRV: on request ! %p", out);

    evhttp_cancel_request (req);
/*
    s3http_client_request_reset (out->http);
    
    s3http_client_set_output_length (out->http, 0);
    s3http_client_add_output_header (out->http, 
        "Content-Length", "0");

    s3http_client_start_request (out->http, S3Method_get, "http://127.0.0.1:8080/index.html");
*/
}

static void on_http_close (S3HttpClient *http, void *ctx)
{
    char c = 'x';
    OutData *out = (OutData *)ctx;
    LOG_debug (HTTP_TEST, "CLI: on close ! %p", out);
    
    s3http_client_request_reset (out->http);
    
 //   s3http_client_set_output_length (out->http, 1);
 //   s3http_client_add_output_data (out->http, &c, 1);

    s3http_client_start_request (out->http, S3Method_get, "http://127.0.0.1:80/index.html");
}

static void run_request_test (struct event_base *evbase, struct evdns_base *dns_base, TestID test_id)
{
    OutData *out;
    struct evbuffer *in_buf;
    char c = 'x';

    LOG_debug (HTTP_TEST, "===================== TEST ID : %d  =======================", test_id);
    out = g_new0 (OutData, 1);
    out->evbase = evbase;
    out->test_id = test_id;

    out->evhttp = evhttp_new (evbase);
    evhttp_bind_socket (out->evhttp, "127.0.0.1", 8080);
    evhttp_set_gencb (out->evhttp, on_request_gencb, out);
    
    //out->http = s3http_client_create (evbase, dns_base);
    in_buf = evbuffer_new ();

    s3http_client_set_cb_ctx (out->http, out);
    s3http_client_set_on_chunk_cb (out->http, on_input_data_cb);
    s3http_client_set_close_cb (out->http, on_http_close);


    //s3http_client_set_output_length (out->http, 1);
    //s3http_client_add_output_data (out->http, &c, 1);

    s3http_client_start_request (out->http, S3Method_get, "http://127.0.0.1:80/index.html");
    
    event_base_dispatch (evbase);
    
    s3http_client_destroy (out->http);

    LOG_debug (HTTP_TEST, "Resulting buff: %zd", evbuffer_get_length (in_buf));
    evbuffer_free (in_buf);

    g_free (out->first_line);
    g_free (out->header_line);
    evconnlistener_free (out->listener);
    evtimer_del (out->timeout);
    event_free (out->timeout);
    evbuffer_free (out->out_buf);
    evbuffer_free (out->in_file);

    g_free (out);
    LOG_debug (HTTP_TEST, "===================== END TEST ID : %d  =======================", test_id);

}

int main (int argc, char *argv[])
{
    struct event_base *evbase;
    struct evdns_base *dns_base;
    int i;
    int test_id = -1;

    event_set_mem_functions (g_malloc, g_realloc, g_free);

    evbase = event_base_new ();
	dns_base = evdns_base_new (evbase, 1);
    
    if (argc > 1)
        test_id = atoi (argv[1]);

    if (test_id >= 0)
        // run_responce_test (evbase, dns_base, test_id);
        run_request_test (evbase, dns_base, test_id);
    else {
        for (i = 0; i < TID_last_test; i++) {
            run_responce_test (evbase, dns_base, i);
        }
    }

    evdns_base_free (dns_base, 0);
    event_base_free (evbase);

    return 0;
}
