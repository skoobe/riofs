#include "s3_http_client.h"

/*{{{ declaration */

// current HTTP state
typedef enum {
    S3S_disconnected = 0,
    S3S_connecting = 1,
    S3S_expected_first_line = 2,
    S3S_expected_headers = 3,
    S3S_expected_data = 4,
} S3HttpState;

// HTTP structure
struct _S3Http {
    struct event_base *evbase;
    struct evdns_base *dns_base;

    S3HttpState state;
    S3HttpRequestMethod method; // HTTP method: GET, PUT, etc

    struct bufferevent *bev;
    gchar *url;
    struct evhttp_uri *http_uri; // parsed URI
    
    // outgoing HTTP request
    GList *l_output_headers;
    struct evbuffer *output_buffer;
    
    // incomming HTTP responce
    GList *l_input_headers;
    struct evbuffer *input_buffer;
    
    // HTTP responce information
    gint response_code;
    gchar *response_code_line;
    gint major; // HTTP version
    gint minor;
    
    // total expected incoming responce data length
    guint64 input_length;
    // read so far
    guint64 input_read;

    // context data for callback functions
    gpointer cb_ctx;
    s3http_on_input_data_cb on_input_data_cb;
    s3http_on_close_cb on_close_cb;
    s3http_on_connection_cb on_connection_cb;
};

typedef struct {
    gchar *key;
    gchar *value;
} S3Header;



static void s3http_read_cb (struct bufferevent *bev, void *ctx);
static void s3http_write_cb (struct bufferevent *bev, void *ctx);
static void s3http_event_cb (struct bufferevent *bev, short what, void *ctx);
static void s3http_send_initial_request (S3Http *http);
static void s3http_free_headers (GList *l_headers);

/*}}}*/

/*{{{ S3Http create / destroy */
S3Http *s3http_new (struct event_base *evbase, struct evdns_base *dns_base, S3RequestMethod method, const gchar *url)
{
    S3Http *http;

    con = g_new0 (S3Http, 1);
    http->evbase = evbase;
    http->dns_base = dns_base;
    http->url = g_strdup (url);
    http->http_uri = evhttp_uri_parse_with_flags (url, 0);
    http->state = S3S_disconnected;
    http->l_output_headers = NULL;
    http->l_input_headers = NULL;
    http->method = method;
    http->response_code_line = NULL;
    http->output_buffer = evbuffer_new ();
    http->input_buffer = evbuffer_new ();
    http->input_length = -1;
    http->input_read = 0;

    http->bev = bufferevent_socket_new (evbase, -1, 0);

    if (!http->bev) {
        return NULL;
    }

    return con;
}

void s3http_destroy (S3Http *http)
{
    bufferevent_free (http->bev);
    evhttp_uri_free (http->http_uri);
    evbuffer_free (http->output_buffer);
    evbuffer_free (http->input_buffer);
    s3http_free_headers (http->l_input_headers);
    s3http_free_headers (http->l_output_headers);
    if (http->response_code_line)
        g_free (http->response_code_line);
    g_free (http->url);
    g_free (con);
}
/*}}}*/

/*{{{ bufferevent callback functions*/

static void s3http_write_cb (struct bufferevent *bev, void *ctx)
{
    S3Http *http = (S3Http *) ctx;
    LOG_debug ("Data sent !");
}

static gboolean s3http_parse_response_line (S3Http *http, char *line)
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

	http->response_code = atoi (number);
	n = sscanf (protocol, "HTTP/%d.%d%c", &major, &minor, &ch);
	if (n != 2 || major > 1) {
		LOG_debug ("bad version %s", line);
		return FALSE;
	}
	http->major = major;
	http->minor = minor;

	if (http->response_code == 0) {
		LOG_debug ("bad response code \"%s\"", number);
		return FALSE;
	}

	if ((http->response_code_line = g_strdup (readable)) == NULL) {
		LOG_err ("strdup");
		return FALSE;
	}

	return TRUE;
}


static size_t s3http_parse_first_line (S3Http *http, struct evbuffer *input_buf)
{
    char *line;
    size_t line_length;

    line = evbuffer_readln (input_buf, &line_length, EVBUFFER_EOL_CRLF);
    LOG_debug ("First Line: (%zd) %s", line_length, line);
    // need more data ?
    if (!line) {
        return 0;
    }

    if (!s3http_parse_response_line (con, line)) {
        g_free (line);
        return 0;
    }

    g_free (line);
    
    return line_length;
}

static gboolean s3http_parse_headers (S3Http *http, struct evbuffer *input_buf)
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
        http->l_input_headers = g_list_append (http->l_input_headers, header);

        LOG_debug ("Adding header !");
        if (!strcmp (skey, "Content-Length")) {
            char *endp;
		    http->input_length = evutil_strtoll (svalue, &endp, 10);
		    if (*svalue == '\0' || *endp != '\0' || http->input_length < 0) {
                LOG_debug ("Illegal content length: %s", svalue);
                http->input_length = -1;
            }
        }

        g_free (line);        
    }
    LOG_debug ("Wrong header line: %s", line);

    // if we are here - not all headers have been received !
    return FALSE;
}

static void s3http_read_cb (struct bufferevent *bev, void *ctx)
{
    S3Http *http = (S3Http *) ctx;
    struct evbuffer *in_buf;

    in_buf = bufferevent_get_input (bev);
    LOG_debug ("Data received: %zd\n=====================================", evbuffer_get_length (in_buf));
 //   LOG_debug ("%s\n=========================================", evbuffer_pullup (in_buf, -1));

    if (http->state == S3S_expected_first_line) {
        size_t first_line_len = s3http_parse_first_line (con, in_buf);
        if (!first_line_len) {
            g_free (http->response_code_line);
            http->response_code_line = NULL;
            LOG_debug ("More first line data expected !");
            return;
        }
        LOG_debug ("Response:  HTTP %d.%d, code: %d, code_line: %s", http->major, http->minor, http->response_code, http->response_code_line);
        http->state = S3S_expected_headers;
    }

    if (http->state == S3S_expected_headers) {
        GList *l;
        if (!s3http_parse_headers (con, in_buf)) {
            LOG_debug ("More headers data expected !");
            s3http_free_headers (http->l_input_headers);
            http->l_input_headers = NULL;
            return;
        }

        http->state = S3S_expected_data;
        LOG_debug ("ALL headers received !");
        for (l = g_list_first (http->l_input_headers); l; l = g_list_next (l)) {
            S3Header *header = (S3Header *) l->data;
            LOG_debug ("\t%s: %s", header->key, header->value);
        }
    }

    if (http->state == S3S_expected_data) {
        http->input_read += evbuffer_get_length (in_buf);
        evbuffer_add_buffer (http->input_buffer, in_buf);
        LOG_debug ("INPUT buf: %zd bytes", evbuffer_get_length (http->input_buffer));
        if (http->on_input_data_cb)
            http->on_input_data_cb (con, http->input_buffer, http->cb_data);
        if (http->input_read == http->input_length) {
            LOG_debug ("DONE downloading !");
            http->state == S3S_expected_first_line;
        }
    }
}

static void s3http_event_cb (struct bufferevent *bev, short what, void *ctx)
{
    S3Http *http = (S3Http *) ctx;
    
    LOG_debug ("Connection event !");

    http->state = S3S_disconnected;

    if (http->on_close_cb)
        http->on_close_cb (con, http->cb_data);
}

static void s3http_connection_event_cb (struct bufferevent *bev, short what, void *ctx)
{
    S3Http *http = (S3Http *) ctx;

	if (!(what & BEV_EVENT_CONNECTED)) {
        // XXX:
        LOG_msg ("Failed to establish connection !");
        return;
    }
    
    LOG_debug ("Connected to the server !");

    http->state = S3S_expected_first_line;

    bufferevent_enable (http->bev, EV_READ);
    bufferevent_setcb (http->bev, 
        s3http_read_cb, s3http_write_cb, s3http_event_cb,
        con
    );
    
    if (http->on_connection_cb)
        http->on_connection_cb (con, http->cb_data);
   // s3http_send_initial_request (con);
}
/*}}}*/

/*{{{ connection */
// connect to the remote server
static void s3http_connect (S3Http *http)
{
    if (http->state == S3S_connecting)
        return;
    
    LOG_debug ("Connecting to %s:%d ..", 
        evhttp_uri_get_host (http->http_uri),
        evhttp_uri_get_port (http->http_uri)
    );

    http->state = S3S_connecting;
    
    bufferevent_enable (http->bev, EV_WRITE);
    bufferevent_setcb (http->bev, 
        NULL, NULL, s3http_connection_event_cb,
        con
    );

    bufferevent_socket_connect_hostname (http->bev, http->dns_base, 
        AF_UNSPEC, 
        evhttp_uri_get_host (http->http_uri),
        evhttp_uri_get_port (http->http_uri)
    );
}

static gboolean s3http_is_connected (S3Http *http)
{
    return (http->state != S3S_disconnected);
}
/*}}}*/

/*{{{ request */
static void s3header_free (S3Header *header)
{
    g_free (header->key);
    g_free (header->value);
    g_free (header);
}

static void s3http_free_headers (GList *l_headers)
{
    GList *l;
    for (l = g_list_first (l_headers); l; l = g_list_next (l)) {
        S3Header *header = (S3Header *) l->data;
        s3header_free (header);
    }

    g_list_free (l_headers);
}

void s3http_add_output_header (S3Http *http, const gchar *key, const gchar *value)
{
    S3Header *header;

    header = g_new0 (S3Header, 1);
    header->key = g_strdup (key);
    header->value = g_strdup (value);

    http->l_output_headers = g_list_append (http->l_output_headers, header);
}

void s3http_add_output_data (S3Http *http, char *buf, size_t size)
{
    evbuffer_add (http->output_buffer, buf, size); 
}

const gchar *s3http_get_input_header (S3Http *http, const gchar *key)
{
    GList *l;
    
    for (l = g_list_first (http->l_input_headers); l; l = g_list_next (l)) {
        S3Header *header = (S3Header *) l->data;
        if (!strcmp (header->key, key))
            return header->value;
    }

    return NULL;
}

gint64 s3http_get_input_length (S3Http *http)
{
    return http->input_length;
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
static void s3http_send_initial_request (S3Http *http)
{
    struct evbuffer *out_buf;
    GList *l;

    out_buf = evbuffer_new ();

    // first line
    evbuffer_add_printf (out_buf, "%s %s HTTP/1.1\n", 
        s3method_to_string (http->method),
        evhttp_uri_get_path (http->http_uri)
    );

    // host
    evbuffer_add_printf (out_buf, "Host: %s\n",
        evhttp_uri_get_host (http->http_uri)
    );

    // add headers
    for (l = g_list_first (http->l_output_headers); l; l = g_list_next (l)) {
        S3Header *header = (S3Header *) l->data;
        evbuffer_add_printf (out_buf, "%s: %s\n",
            header->key, header->value
        );
    }

    // end line
    evbuffer_add_printf (out_buf, "\n");

    // add current data in the output buffer
    evbuffer_add_buffer (out_buf, http->output_buffer);
    
    LOG_debug ("Sending initial request:\n==========================");
    LOG_debug ("%s\n===========================", evbuffer_pullup (out_buf, -1));
    // send it
    bufferevent_write_buffer (http->bev, out_buf);

    // free memory
    evbuffer_free (out_buf);
}

gboolean s3http_start_request (S3Http *http)
{
    // connect if it's not
    if (!s3http_is_connected (con)) {
        s3http_connect (con);
    } else {
        s3http_send_initial_request (con);
    }

    return TRUE;
}
/*}}}*/


void s3http_set_cb_data (S3Http *http, gpointer ctx)
{
    http->cb_data = ctx;
}

void s3http_set_input_data_cb (S3Http *http,  s3http_on_input_data_cb on_input_data_cb)
{
    http->on_input_data_cb = on_input_data_cb;
}

void s3http_set_close_cb (S3Http *http, s3http_on_close_cb on_close_cb)
{
    http->on_close_cb = on_close_cb;
}

void s3http_set_connection_cb (S3Http *http, s3http_on_connection_cb on_connection_cb)
{
    http->on_connection_cb = on_connection_cb;
}

