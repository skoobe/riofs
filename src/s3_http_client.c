#include "s3_http_client.h"

/*{{{ declaration */
typedef enum {
    S3S_disconnected = 0,
    S3S_connecting = 1,
    S3S_expected_first_line = 2,
    S3S_expected_headers = 3,
    S3S_expected_data = 4,
} S3State;

struct _S3Connection {
    struct event_base *evbase;
    struct evdns_base *dns_base;

    S3State state;
    S3RequestMethod method;

    struct bufferevent *bev;
    gchar *url;
    struct evhttp_uri *http_uri;
    
    GList *l_output_headers;
    GList *l_input_headers;
    struct evbuffer *output_buffer;
    struct evbuffer *input_buffer;

    gint response_code;
    gint major;
    gint minor;
    gchar *response_code_line;

    guint64 input_length;
    guint64 input_read;

    gpointer cb_data;
    S3Connection_on_input_data_cb on_input_data_cb;
    S3Connection_on_close_cb on_close_cb;
    S3Connection_on_connection_cb on_connection_cb;
};

typedef struct {
    gchar *key;
    gchar *value;
} S3Header;



static void s3connection_read_cb (struct bufferevent *bev, void *ctx);
static void s3connection_write_cb (struct bufferevent *bev, void *ctx);
static void s3connection_event_cb (struct bufferevent *bev, short what, void *ctx);
static void s3connection_send_initial_request (S3Connection *con);
static void s3connection_free_headers (GList *l_headers);

/*}}}*/

/*{{{ S3Connection create / destroy */
S3Connection *s3connection_new (struct event_base *evbase, struct evdns_base *dns_base, S3RequestMethod method, const gchar *url)
{
    S3Connection *con;

    con = g_new0 (S3Connection, 1);
    con->evbase = evbase;
    con->dns_base = dns_base;
    con->url = g_strdup (url);
    con->http_uri = evhttp_uri_parse_with_flags (url, 0);
    con->state = S3S_disconnected;
    con->l_output_headers = NULL;
    con->l_input_headers = NULL;
    con->method = method;
    con->response_code_line = NULL;
    con->output_buffer = evbuffer_new ();
    con->input_buffer = evbuffer_new ();
    con->input_length = -1;
    con->input_read = 0;

    con->bev = bufferevent_socket_new (evbase, -1, 0);

    if (!con->bev) {
        return NULL;
    }

    return con;
}

void s3connection_destroy (S3Connection *con)
{
    bufferevent_free (con->bev);
    evhttp_uri_free (con->http_uri);
    evbuffer_free (con->output_buffer);
    evbuffer_free (con->input_buffer);
    s3connection_free_headers (con->l_input_headers);
    s3connection_free_headers (con->l_output_headers);
    if (con->response_code_line)
        g_free (con->response_code_line);
    g_free (con->url);
    g_free (con);
}
/*}}}*/

/*{{{ bufferevent callback functions*/

static void s3connection_write_cb (struct bufferevent *bev, void *ctx)
{
    S3Connection *con = (S3Connection *) ctx;
    LOG_debug ("Data sent !");
}

static gboolean s3connection_parse_response_line (S3Connection *con, char *line)
{
	char *protocol;
	char *number;
	const char *readable = "";
	int major, minor;
	char ch;
    int n;

	protocol = strsep(&line, " ");
	if (line == NULL)
		return FALSE;
	number = strsep(&line, " ");
	if (line != NULL)
		readable = line;

	con->response_code = atoi (number);
	n = sscanf (protocol, "HTTP/%d.%d%c", &major, &minor, &ch);
	if (n != 2 || major > 1) {
		LOG_debug ("bad version %s", line);
		return FALSE;
	}
	con->major = major;
	con->minor = minor;

	if (con->response_code == 0) {
		LOG_debug ("bad response code \"%s\"", number);
		return FALSE;
	}

	if ((con->response_code_line = g_strdup (readable)) == NULL) {
		LOG_err ("strdup");
		return FALSE;
	}

	return TRUE;
}


static size_t s3connection_parse_first_line (S3Connection *con, struct evbuffer *input_buf)
{
    char *line;
    size_t line_length;

    line = evbuffer_readln (input_buf, &line_length, EVBUFFER_EOL_CRLF);
    LOG_debug ("First Line: (%zd) %s", line_length, line);
    // need more data ?
    if (!line) {
        return 0;
    }

    if (!s3connection_parse_response_line (con, line)) {
        g_free (line);
        return 0;
    }

    g_free (line);
    
    return line_length;
}

static gboolean s3connection_parse_headers (S3Connection *con, struct evbuffer *input_buf)
{
    size_t line_length;
    char *line;
    S3Header *header;

	while ((line = evbuffer_readln (input_buf, &line_length, EVBUFFER_EOL_CRLF)) != NULL) {
		char *skey, *svalue;
        
        LOG_debug ("Line: >>%s<<", line);
        // last line
		if (*line == '\0') {
            LOG_debug ("Last header line !");
			g_free (line);
			return TRUE;
		}

        LOG_debug ("HEADER line: %s\n", line);

		svalue = line;
		skey = strsep(&svalue, ":");
		if (svalue == NULL) {
            LOG_debug ("Wrong header !");
	        g_free (line);
			return TRUE;
        }

		svalue += strspn(svalue, " ");

        header = g_new0 (S3Header, 1);
        header->key = g_strdup (skey);
        header->value = g_strdup (svalue);
        con->l_input_headers = g_list_append (con->l_input_headers, header);

        LOG_debug ("Adding header !");
        if (!strcmp (skey, "Content-Length")) {
            char *endp;
		    con->input_length = evutil_strtoll (svalue, &endp, 10);
		    if (*svalue == '\0' || *endp != '\0' || con->input_length < 0) {
                LOG_debug ("Illegal content length: %s", svalue);
                con->input_length = -1;
            }
        }

        g_free (line);        
    }
    LOG_debug ("Wrong header line: %s", line);

    // if we are here - not all headers have been received !
    return FALSE;
}

static void s3connection_read_cb (struct bufferevent *bev, void *ctx)
{
    S3Connection *con = (S3Connection *) ctx;
    struct evbuffer *in_buf;

    in_buf = bufferevent_get_input (bev);
    LOG_debug ("Data received: %zd\n=====================================", evbuffer_get_length (in_buf));
 //   LOG_debug ("%s\n=========================================", evbuffer_pullup (in_buf, -1));

    if (con->state == S3S_expected_first_line) {
        size_t first_line_len = s3connection_parse_first_line (con, in_buf);
        if (!first_line_len) {
            g_free (con->response_code_line);
            con->response_code_line = NULL;
            LOG_debug ("More first line data expected !");
            return;
        }
        LOG_debug ("Response:  HTTP %d.%d, code: %d, code_line: %s", con->major, con->minor, con->response_code, con->response_code_line);
        con->state = S3S_expected_headers;
    }

    if (con->state == S3S_expected_headers) {
        GList *l;
        if (!s3connection_parse_headers (con, in_buf)) {
            LOG_debug ("More headers data expected !");
            s3connection_free_headers (con->l_input_headers);
            con->l_input_headers = NULL;
            return;
        }

        con->state = S3S_expected_data;
        LOG_debug ("ALL headers received !");
        for (l = g_list_first (con->l_input_headers); l; l = g_list_next (l)) {
            S3Header *header = (S3Header *) l->data;
            LOG_debug ("\t%s: %s", header->key, header->value);
        }
    }

    if (con->state == S3S_expected_data) {
        con->input_read += evbuffer_get_length (in_buf);
        evbuffer_add_buffer (con->input_buffer, in_buf);
        LOG_debug ("INPUT buf: %zd bytes", evbuffer_get_length (con->input_buffer));
        if (con->on_input_data_cb)
            con->on_input_data_cb (con, con->input_buffer, con->cb_data);
        if (con->input_read == con->input_length) {
            LOG_debug ("DONE downloading !");
            con->state == S3S_expected_first_line;
        }
    }
}

static void s3connection_event_cb (struct bufferevent *bev, short what, void *ctx)
{
    S3Connection *con = (S3Connection *) ctx;
    
    LOG_debug ("Connection event !");

    con->state = S3S_disconnected;

    if (con->on_close_cb)
        con->on_close_cb (con, con->cb_data);
}

static void s3connection_connection_event_cb (struct bufferevent *bev, short what, void *ctx)
{
    S3Connection *con = (S3Connection *) ctx;

	if (!(what & BEV_EVENT_CONNECTED)) {
        // XXX:
        LOG_err ("Unexpected event !");
        return;
    }
    
    LOG_debug ("Connected to the server !");

    con->state = S3S_expected_first_line;

    bufferevent_enable (con->bev, EV_READ);
    bufferevent_setcb (con->bev, 
        s3connection_read_cb, s3connection_write_cb, s3connection_event_cb,
        con
    );
    
    if (con->on_connection_cb)
        con->on_connection_cb (con, con->cb_data);
   // s3connection_send_initial_request (con);
}
/*}}}*/

/*{{{ connection */
// connect to the remote server
static void s3connection_connect (S3Connection *con)
{
    if (con->state == S3S_connecting)
        return;
    
    LOG_debug ("Connecting to %s:%d ..", 
        evhttp_uri_get_host (con->http_uri),
        evhttp_uri_get_port (con->http_uri)
    );

    con->state = S3S_connecting;
    
    bufferevent_enable (con->bev, EV_WRITE);
    bufferevent_setcb (con->bev, 
        NULL, NULL, s3connection_connection_event_cb,
        con
    );

    bufferevent_socket_connect_hostname (con->bev, con->dns_base, 
        AF_UNSPEC, 
        evhttp_uri_get_host (con->http_uri),
        evhttp_uri_get_port (con->http_uri)
    );
}

static gboolean s3connection_is_connected (S3Connection *con)
{
    return (con->state != S3S_disconnected);
}
/*}}}*/

/*{{{ request */
static void s3header_free (S3Header *header)
{
    g_free (header->key);
    g_free (header->value);
    g_free (header);
}

static void s3connection_free_headers (GList *l_headers)
{
    GList *l;
    for (l = g_list_first (l_headers); l; l = g_list_next (l)) {
        S3Header *header = (S3Header *) l->data;
        s3header_free (header);
    }

    g_list_free (l_headers);
}

void s3connection_add_output_header (S3Connection *con, const gchar *key, const gchar *value)
{
    S3Header *header;

    header = g_new0 (S3Header, 1);
    header->key = g_strdup (key);
    header->value = g_strdup (value);

    con->l_output_headers = g_list_append (con->l_output_headers, header);
}

void s3connection_add_output_data (S3Connection *con, char *buf, size_t size)
{
    evbuffer_add (con->output_buffer, buf, size); 
}

const gchar *s3connection_get_input_header (S3Connection *con, const gchar *key)
{
    GList *l;
    
    for (l = g_list_first (con->l_input_headers); l; l = g_list_next (l)) {
        S3Header *header = (S3Header *) l->data;
        if (!strcmp (header->key, key))
            return header->value;
    }

    return NULL;
}

gint64 s3connection_get_input_length (S3Connection *con)
{
    return con->input_length;
}

static const gchar *s3method_to_string (S3RequestMethod method)
{
    switch (method) {
        case S3Method_get:
            return "GET";
        case S3Method_put:
            return "PUT";
        default:
            return "GET";
    }
}

// create HTTP headers and send first part of buffer
static void s3connection_send_initial_request (S3Connection *con)
{
    struct evbuffer *out_buf;
    GList *l;

    out_buf = evbuffer_new ();

    // first line
    evbuffer_add_printf (out_buf, "%s %s HTTP/1.1\n", 
        s3method_to_string (con->method),
        evhttp_uri_get_path (con->http_uri)
    );

    // host
    evbuffer_add_printf (out_buf, "Host: %s\n",
        evhttp_uri_get_host (con->http_uri)
    );

    // add headers
    for (l = g_list_first (con->l_output_headers); l; l = g_list_next (l)) {
        S3Header *header = (S3Header *) l->data;
        evbuffer_add_printf (out_buf, "%s: %s\n",
            header->key, header->value
        );
    }

    // end line
    evbuffer_add_printf (out_buf, "\n");

    // add current data in the output buffer
    evbuffer_add_buffer (out_buf, con->output_buffer);
    
    LOG_debug ("Sending initial request:\n==========================");
    LOG_debug ("%s\n===========================", evbuffer_pullup (out_buf, -1));
    // send it
    bufferevent_write_buffer (con->bev, out_buf);

    // free memory
    evbuffer_free (out_buf);
}

gboolean s3connection_start_request (S3Connection *con)
{
    // connect if it's not
    if (!s3connection_is_connected (con)) {
        s3connection_connect (con);
    } else {
        s3connection_send_initial_request (con);
    }

    return TRUE;
}
/*}}}*/


void s3connection_set_cb_data (S3Connection *con, gpointer ctx)
{
    con->cb_data = ctx;
}

void s3connection_set_input_data_cb (S3Connection *con,  S3Connection_on_input_data_cb on_input_data_cb)
{
    con->on_input_data_cb = on_input_data_cb;
}

void s3connection_set_close_cb (S3Connection *con, S3Connection_on_close_cb on_close_cb)
{
    con->on_close_cb = on_close_cb;
}

void s3connection_set_connection_cb (S3Connection *con, S3Connection_on_connection_cb on_connection_cb)
{
    con->on_connection_cb = on_connection_cb;
}

