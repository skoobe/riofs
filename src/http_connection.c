/*
 * Copyright (C) 2012-2013 Paul Ionkin <paul.ionkin@gmail.com>
 * Copyright (C) 2012-2013 Skoobe GmbH. All rights reserved.
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
#include "http_connection.h"
#include "utils.h"
#include "stat_srv.h"

/*{{{ struct*/

// HTTP header: key, value
typedef struct {
    gchar *key;
    gchar *value;
} HttpConnectionHeader;

#define CON_LOG "con"
#define CMD_IDLE 9999

static void http_connection_on_close (struct evhttp_connection *evcon, void *ctx);
static gboolean http_connection_init (HttpConnection *con);

/*}}}*/

/*{{{ create / destroy */
// create HttpConnection object
// establish HTTP connections to 
gpointer http_connection_create (Application *app)
{
    HttpConnection *con;

    con = g_new0 (HttpConnection, 1);
    if (!con) {
        LOG_err (CON_LOG, "Failed to create HttpConnection !");
        return NULL;
    }
    
    con->app = app;
    con->l_output_headers = NULL;
    con->cur_cmd_type = CMD_IDLE;
    con->cur_url = NULL;
    con->cur_time_start = 0;
    con->cur_time_stop = 0;
    con->jobs_nr = 0;
    con->connects_nr = 0;
    con->cur_code = 0;
    con->total_bytes_out = con->total_bytes_in = 0;
    con->errors_nr = 0;

    con->is_acquired = FALSE;
    
    if (!http_connection_init (con)) {
        g_free (con);
        return NULL;
    }

    return (gpointer)con;
}

static gboolean http_connection_init (HttpConnection *con)
{
    LOG_debug (CON_LOG, CON_H"Connecting to %s:%d", con,
        conf_get_string (application_get_conf (con->app), "s3.host"),
        conf_get_int (application_get_conf (con->app), "s3.port")
    );

    con->connects_nr++;

    if (con->evcon)
        evhttp_connection_free (con->evcon);

#ifdef SSL_ENABLED
    if (conf_get_boolean (application_get_conf (con->app), "s3.ssl")) {
        SSL *ssl;
        struct bufferevent *bev;
        
        ssl = SSL_new (application_get_ssl_ctx (con->app));
        if (!ssl) {
            LOG_err (CON_LOG, CON_H"Failed to create SSL connection: %s", con,
                ERR_reason_error_string (ERR_get_error ()));
            return FALSE;
        }

        bev = bufferevent_openssl_socket_new (
            application_get_evbase (con->app),
            -1, ssl, 
            BUFFEREVENT_SSL_CONNECTING,
            BEV_OPT_CLOSE_ON_FREE | BEV_OPT_DEFER_CALLBACKS
        );

        if (!bev) {
            LOG_err (CON_LOG, CON_H"Failed to create SSL connection !", con);
            return FALSE;
        }

        con->evcon = evhttp_connection_base_bufferevent_new (
            application_get_evbase (con->app),
            application_get_dnsbase (con->app),
            bev,
            conf_get_string (application_get_conf (con->app), "s3.host"),
            conf_get_int (application_get_conf (con->app), "s3.port")
        );
    } else {
#endif
    con->evcon = evhttp_connection_base_new (
        application_get_evbase (con->app),
        application_get_dnsbase (con->app),
        conf_get_string (application_get_conf (con->app), "s3.host"),
        conf_get_int (application_get_conf (con->app), "s3.port")
    );
#ifdef SSL_ENABLED
    }
#endif

    if (!con->evcon) {
        LOG_err (CON_LOG, "Failed to create evhttp_connection !");
        return FALSE;
    }
    
    evhttp_connection_set_timeout (con->evcon, conf_get_int (application_get_conf (con->app), "connection.timeout"));
    evhttp_connection_set_retries (con->evcon, conf_get_int (application_get_conf (con->app), "connection.retries"));

    evhttp_connection_set_closecb (con->evcon, http_connection_on_close, con);
    
    return TRUE;
}

// destory HttpConnection)
void http_connection_destroy (gpointer data)
{
    HttpConnection *con = (HttpConnection *) data;
    
    if (con->cur_url)
        g_free (con->cur_url);
    if (con->evcon)
        evhttp_connection_free (con->evcon);
    g_free (con);
}
/*}}}*/

void http_connection_set_on_released_cb (gpointer client, ClientPool_on_released_cb client_on_released_cb, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;

    con->client_on_released_cb = client_on_released_cb;
    con->pool_ctx = ctx;
}

gboolean http_connection_check_rediness (gpointer client)
{
    HttpConnection *con = (HttpConnection *) client;

    return !con->is_acquired;
}

gboolean http_connection_acquire (HttpConnection *con)
{
    con->is_acquired = TRUE;

    LOG_debug (CON_LOG, CON_H"Connection object is acquired!", con);

    return TRUE;
}

gboolean http_connection_release (HttpConnection *con)
{
    con->is_acquired = FALSE;

    LOG_debug (CON_LOG, CON_H"Connection object is released!", con);

    if (con->client_on_released_cb)
        con->client_on_released_cb (con, con->pool_ctx);
    
    return TRUE;
}

// callback connection is closed
static void http_connection_on_close (G_GNUC_UNUSED struct evhttp_connection *evcon, void *ctx)
{
    HttpConnection *con = (HttpConnection *) ctx;

    LOG_debug (CON_LOG, CON_H"Connection closed !", con);

    //con->cur_cmd_type = CMD_IDLE;
    
    //XXX: need further investigation !
    //con->evcon = NULL;
}

/*{{{ getters */
Application *http_connection_get_app (HttpConnection *con)
{
    return con->app;
}

struct evhttp_connection *http_connection_get_evcon (HttpConnection *con)
{
    return con->evcon;
}

/*}}}*/

/*{{{ get_auth_string */
// create  auth string
// http://docs.amazonwebservices.com/Amazon/2006-03-01/dev/RESTAuthentication.html
static gchar *http_connection_get_auth_string (Application *app, 
        const gchar *method, const gchar *content_type, const gchar *resource, const gchar *time_str,
        GList *l_output_headers)
{
    gchar *string_to_sign;
    unsigned int md_len;
    unsigned char md[EVP_MAX_MD_SIZE];
    gchar *tmp;
    GList *l;
    GString *s_headers;
    gchar *content_md5 = NULL;

    s_headers = g_string_new ("");
    for (l = g_list_first (l_output_headers); l; l = g_list_next (l)) {
        HttpConnectionHeader *header = (HttpConnectionHeader *) l->data;

        if (!strncmp ("Content-MD5", header->key, strlen ("Content-MD5"))) {
            if (content_md5)
                g_free (content_md5);
            content_md5 = g_strdup (header->value);
        // select all HTTP request headers that start with 'x-amz-' (using a case-insensitive comparison)
        } else if (strcasestr (header->key, "x-amz-")) {
            g_string_append_printf (s_headers, "%s:%s\n", header->key, header->value);
        }
    }

    if (!content_md5)
        content_md5 = g_strdup ("");

    // The list of sub-resources that must be included when constructing the CanonicalizedResource 
    // Element are: acl, lifecycle, location, logging, notification, partNumber, policy, 
    // requestPayment, torrent, uploadId, uploads, versionId, versioning, versions and website.
    if (strlen (resource) > 2 && resource[1] == '?') {
        if (strstr (resource, "?acl") || strstr (resource, "?versioning") || strstr (resource, "?versions"))
            tmp = g_strdup_printf ("/%s%s", conf_get_string (application_get_conf (app), "s3.bucket_name"), resource);
        else
            tmp = g_strdup_printf ("/%s/", conf_get_string (application_get_conf (app), "s3.bucket_name"));
    } else
        tmp = g_strdup_printf ("/%s%s", conf_get_string (application_get_conf (app), "s3.bucket_name"), resource);

    string_to_sign = g_strdup_printf (
        "%s\n"  // HTTP-Verb + "\n"
        "%s\n"  // Content-MD5 + "\n"
        "%s\n"  // Content-Type + "\n"
        "%s\n"  // Date + "\n" 
        "%s"    // CanonicalizedAmzHeaders
        "%s",    // CanonicalizedResource

        method, content_md5, content_type, time_str, s_headers->str, tmp
    );
    g_string_free (s_headers, TRUE);
    g_free (content_md5);

    g_free (tmp);

    //LOG_debug (CON_LOG, "%s %s", string_to_sign, conf_get_string (application_get_conf (con->app), "s3.secret_access_key"));

    HMAC (EVP_sha1(),
        conf_get_string (application_get_conf (app), "s3.secret_access_key"),
        strlen (conf_get_string (application_get_conf (app), "s3.secret_access_key")),
        (unsigned char *)string_to_sign, strlen (string_to_sign),
        md, &md_len
    );
    g_free (string_to_sign);

    return get_base64 ((const gchar *)md, md_len);
}
/*}}}*/

static gchar *get_endpoint (const char *xml, size_t xml_len) {
    xmlDocPtr doc;
    xmlXPathContextPtr ctx;
    xmlXPathObjectPtr endpoint_xp;
    xmlNodeSetPtr nodes;
    gchar *endpoint = NULL;

    doc = xmlReadMemory (xml, xml_len, "", NULL, 0);
    ctx = xmlXPathNewContext (doc);
    endpoint_xp = xmlXPathEvalExpression ((xmlChar *) "/Error/Endpoint", ctx);
    nodes = endpoint_xp->nodesetval;

    if (!nodes || nodes->nodeNr < 1) {
        endpoint = NULL;
    } else {
        endpoint = (char *) xmlNodeListGetString (doc, nodes->nodeTab[0]->xmlChildrenNode, 1);
    }

    xmlXPathFreeObject (endpoint_xp);
    xmlXPathFreeContext (ctx);
    xmlFreeDoc (doc);

    return endpoint;
}

typedef struct {
    HttpConnection *con;
    HttpConnection_responce_cb responce_cb;
    gpointer ctx;

    // number of redirects so far
    gint redirects;

    // original values
    gchar *resource_path;
    gchar *http_cmd;
    struct evbuffer *out_buffer;
    size_t out_size;

    struct timeval start_tv;
} RequestData;

static void request_data_free (RequestData *data)
{
    g_free (data->resource_path);
    g_free (data->http_cmd);
    g_free (data);
}

static void http_connection_on_responce_cb (struct evhttp_request *req, void *ctx)
{
    RequestData *data = (RequestData *) ctx;
    HttpConnection *con;
    struct evbuffer *inbuf;
    const char *buf = NULL;
    size_t buf_len = 0;
    struct timeval end_tv;
    gchar *s_history;
    char ts[50];
    guint diff_sec = 0;
    const gchar *range_str = NULL;

    con = data->con;
    
    gettimeofday (&end_tv, NULL);
    con->cur_time_stop = time (NULL);

    if (con->cur_time_start) {
        struct tm *cur_p;
        struct tm cur;
        
        localtime_r (&con->cur_time_start, &cur);
        cur_p = &cur;
        if (!strftime (ts, sizeof (ts), "%H:%M:%S", cur_p))
            ts[0] = '\0';
    } else {
        strcpy (ts, "");
    }

    if (con->cur_time_start && con->cur_time_start < con->cur_time_stop)
        diff_sec = con->cur_time_stop - con->cur_time_start;
    
    if (req) {
        struct evkeyvalq *output_headers;
        struct evkeyvalq *input_headers;
        struct evkeyval *header;

        inbuf = evhttp_request_get_input_buffer (req);
        buf_len = evbuffer_get_length (inbuf);
        
        output_headers = evhttp_request_get_output_headers (req);
        if (output_headers)
            range_str = evhttp_find_header (output_headers, "Range");

        con->cur_code = evhttp_request_get_response_code (req);

        // get the size of Output headers 
        TAILQ_FOREACH(header, output_headers, next) {
            con->total_bytes_out += strlen (header->key) + strlen (header->value);
        }

        // get the size of Input headers 
        input_headers = evhttp_request_get_input_headers (req);
        TAILQ_FOREACH(header, input_headers, next) {
            con->total_bytes_in += strlen (header->key) + strlen (header->value);
        }

        con->total_bytes_in += buf_len;

    } else {
        con->cur_code = 500;
    }
    
    s_history = g_strdup_printf ("[%p] %s (%u sec) %s %s %s   HTTP Code: %d (Sent: %zu Received: %zu bytes)", 
        con,
        ts, diff_sec, data->http_cmd, data->con->cur_url, 
        range_str ? range_str : "",
        con->cur_code,
        data->out_size,
        buf_len
    );
    
    stats_srv_add_op_history (application_get_stat_srv (data->con->app), s_history);
    g_free (s_history);

    LOG_debug (CON_LOG, CON_H"Got HTTP response from server! (%"G_GUINT64_FORMAT"msec)", 
        con, timeval_diff (&data->start_tv, &end_tv));

    if (!req) {
        LOG_err (CON_LOG, CON_H"Request failed !", con);
        con->errors_nr++;

#ifdef SSL_ENABLED
    { unsigned long oslerr;
      while ((oslerr = bufferevent_get_openssl_error (evhttp_connection_get_bufferevent (data->con->evcon))))
        { char b[128];
          ERR_error_string_n (oslerr, b, sizeof (b));
          LOG_err (CON_LOG, CON_H"SSL error: %s", con, b);
        }
    }
#endif
        if (data->responce_cb)
            data->responce_cb (data->con, data->ctx, FALSE, NULL, 0, NULL);
        goto done;
    }
    
    // check if we reached maximum redirect count
    if (data->redirects > conf_get_int (application_get_conf (data->con->app), "connection.max_redirects")) {
        LOG_err (CON_LOG, CON_H"Too many redirects !", con);
        con->errors_nr++;
        if (data->responce_cb)
            data->responce_cb (data->con, data->ctx, FALSE, NULL, 0, NULL);
        goto done;
    }

    // handle redirect
    if (evhttp_request_get_response_code (req) == 301) {
        const gchar *loc;
        struct evkeyvalq *headers;

        data->redirects++;
        headers = evhttp_request_get_input_headers (req);

        loc = http_find_header (headers, "Location");
        if (!loc) {
            inbuf = evhttp_request_get_input_buffer (req);
            buf_len = evbuffer_get_length (inbuf);
            buf = (const char *) evbuffer_pullup (inbuf, buf_len);

            // let's parse XML
            loc = get_endpoint (buf, buf_len);
            
            if (!loc) {
                LOG_err (CON_LOG, CON_H"Redirect URL not found !", con);
                if (data->responce_cb)
                    data->responce_cb (data->con, data->ctx, FALSE, NULL, 0, NULL);
                goto done;
            }
        }

        LOG_debug (CON_LOG, CON_H"New URL: %s", con, loc);

        if (!application_set_url (data->con->app, loc)) {
            if (data->responce_cb)
                data->responce_cb (data->con, data->ctx, FALSE, NULL, 0, NULL);
            goto done;
        }
        //XXX: free loc if it's parsed from xml
        // xmlFree (loc)

        if (!http_connection_init (data->con)) {
            if (data->responce_cb)
                data->responce_cb (data->con, data->ctx, FALSE, NULL, 0, NULL);
            goto done;
        }

        // re-send request
        http_connection_make_request (data->con, data->resource_path, data->http_cmd, data->out_buffer,
            data->responce_cb, data->ctx);
        goto done;
    }
    
    inbuf = evhttp_request_get_input_buffer (req);
    buf_len = evbuffer_get_length (inbuf);
    buf = (const char *) evbuffer_pullup (inbuf, buf_len);

    // OK codes are:
    // 200
    // 204 (No Content)
    // 206 (Partial Content)
    if (evhttp_request_get_response_code (req) != 200 && 
        evhttp_request_get_response_code (req) != 204 && 
        evhttp_request_get_response_code (req) != 206) {
        LOG_debug (CON_LOG, CON_H"Server returned HTTP error: %d (%s) !", 
            con, evhttp_request_get_response_code (req), req->response_code_line);
        
        // if it contains any readable information
        if (buf_len > 1) {
            gchar *tmp;
            tmp = g_new0 (gchar, buf_len + 1);
            strncpy (tmp, buf, buf_len);
            LOG_debug (CON_LOG, CON_H"Error msg: =====\n%s\n=====", con, tmp);
            g_free (tmp);
        }
        
        con->errors_nr++;
        if (data->responce_cb)
            data->responce_cb (data->con, data->ctx, FALSE, NULL, 0, NULL);
        goto done;
    }

    
    if (data->responce_cb)
        data->responce_cb (data->con, data->ctx, TRUE, buf, buf_len, evhttp_request_get_input_headers (req));
    else
        LOG_debug (CON_LOG, CON_H"NO callback function !", con);

done:
    request_data_free (data);
}

static gint hdr_compare (const HttpConnectionHeader *a, const HttpConnectionHeader *b)
{
    return strcmp (a->key, b->key);
}
// add an header to the outgoing request
void http_connection_add_output_header (HttpConnection *con, const gchar *key, const gchar *value)
{
    HttpConnectionHeader *header;

    header = g_new0 (HttpConnectionHeader, 1);
    header->key = g_strdup (key);
    header->value = g_strdup (value);

    con->l_output_headers = g_list_insert_sorted (con->l_output_headers, header, (GCompareFunc) hdr_compare);
}

static void http_connection_free_headers (GList *l_headers)
{
    GList *l;
    for (l = g_list_first (l_headers); l; l = g_list_next (l)) {
        HttpConnectionHeader *header = (HttpConnectionHeader *) l->data;
        g_free (header->key);
        g_free (header->value);
        g_free (header);
    }

    g_list_free (l_headers);
}

gboolean http_connection_make_request (HttpConnection *con, 
    const gchar *resource_path,
    const gchar *http_cmd,
    struct evbuffer *out_buffer,
    HttpConnection_responce_cb responce_cb,
    gpointer ctx)
{
    gchar *auth_str;
    struct evhttp_request *req;
    gchar auth_key[300];
    time_t t;
    char time_str[50];
    RequestData *data;
    int res;
    enum evhttp_cmd_type cmd_type;
    gchar *request_str = NULL;
    GList *l;

    if (!con->evcon)
        if (!http_connection_init (con)) {
            LOG_err (CON_LOG, CON_H"Failed to init HTTP connection !", con);
            if (responce_cb)
                responce_cb (con, ctx, FALSE, NULL, 0, NULL);
            return FALSE;
        }

    data = g_new0 (RequestData, 1);
    data->responce_cb = responce_cb;
    data->ctx = ctx;
    data->con = con;
    data->redirects = 0;
    data->resource_path = url_escape (resource_path);
    data->http_cmd = g_strdup (http_cmd);
    data->out_buffer = out_buffer;
    if (out_buffer)
        data->out_size = evbuffer_get_length (out_buffer);
    else
        data->out_size = 0;
    
    if (!strcasecmp (http_cmd, "GET")) {
        cmd_type = EVHTTP_REQ_GET;
    } else if (!strcasecmp (http_cmd, "PUT")) {
        cmd_type = EVHTTP_REQ_PUT;
    } else if (!strcasecmp (http_cmd, "POST")) {
        cmd_type = EVHTTP_REQ_POST;
    } else if (!strcasecmp (http_cmd, "DELETE")) {
        cmd_type = EVHTTP_REQ_DELETE;
    } else if (!strcasecmp (http_cmd, "HEAD")) {
        cmd_type = EVHTTP_REQ_HEAD;
    } else {
        LOG_err (CON_LOG, CON_H"Unsupported HTTP method: %s", con, http_cmd);
        if (data->responce_cb)
            data->responce_cb (data->con, data->ctx, FALSE, NULL, 0, NULL);
        request_data_free (data);
        return FALSE;
    }
    
    t = time (NULL);
    strftime (time_str, sizeof (time_str), "%a, %d %b %Y %H:%M:%S GMT", gmtime(&t));
    auth_str = http_connection_get_auth_string (con->app, http_cmd, "", data->resource_path, time_str, con->l_output_headers);
    snprintf (auth_key, sizeof (auth_key), "AWS %s:%s", conf_get_string (application_get_conf (con->app), "s3.access_key_id"), auth_str);
    g_free (auth_str);

    req = evhttp_request_new (http_connection_on_responce_cb, data);
    if (!req) {
        LOG_err (CON_LOG, CON_H"Failed to create HTTP request object !", con);
        if (data->responce_cb)
            data->responce_cb (data->con, data->ctx, FALSE, NULL, 0, NULL);

        request_data_free (data);
        return FALSE;
    }

    evhttp_add_header (req->output_headers, "Authorization", auth_key);
    evhttp_add_header (req->output_headers, "Host", conf_get_string (application_get_conf (con->app), "s3.host"));
    evhttp_add_header (req->output_headers, "Date", time_str);
    // ask to keep connection opened
    evhttp_add_header (req->output_headers, "Connection", "keep-alive");
    evhttp_add_header (req->output_headers, "Accept-Encoding", "identify");

    // add headers
    for (l = g_list_first (con->l_output_headers); l; l = g_list_next (l)) {
        HttpConnectionHeader *header = (HttpConnectionHeader *) l->data;
        evhttp_add_header (req->output_headers, 
            header->key, header->value
        );
    }

    http_connection_free_headers (con->l_output_headers);
    con->l_output_headers = NULL;


    if (out_buffer) {
        con->total_bytes_out += evbuffer_get_length (out_buffer);
        evbuffer_add_buffer (req->output_buffer, out_buffer);
    }

    if (conf_get_boolean (application_get_conf (con->app), "s3.path_style")) {
        request_str = g_strdup_printf ("/%s%s", conf_get_string (application_get_conf (con->app), "s3.bucket_name"), data->resource_path);
    } else {
        request_str = g_strdup_printf ("%s", data->resource_path);
    }

    LOG_msg (CON_LOG, CON_H"%s %s  bucket: %s, host: %s", con,
        http_cmd, request_str, 
        conf_get_string (application_get_conf (con->app), "s3.bucket_name"), conf_get_string (application_get_conf (con->app), "s3.host"));
    
    // update stats info
    con->cur_cmd_type = cmd_type;
    if (con->cur_url)
        g_free (con->cur_url);
    con->cur_url = g_strdup_printf ("%s%s%s", "http://", conf_get_string (application_get_conf (con->app), "s3.host"), request_str);

    gettimeofday (&data->start_tv, NULL);
    con->cur_time_start = time (NULL);
    con->jobs_nr++;

    res = evhttp_make_request (http_connection_get_evcon (con), req, cmd_type, request_str);
    g_free (request_str);

    if (res < 0) {
        request_data_free (data);
        if (data->responce_cb)
            data->responce_cb (data->con, data->ctx, FALSE, NULL, 0, NULL);
        return FALSE;
    } else
        return TRUE;
}

// return string with various statistics information
void http_connection_get_stats_info_caption (G_GNUC_UNUSED gpointer client, GString *str, struct PrintFormat *print_format)
{
    g_string_append_printf (str, 
        "%s ID %s Current state %s Last CMD %s Last URL %s Last Code %s Time started (Response sec) %s Jobs (Errors) %s Connects Nr %s Total Out %s Total In %s"
        , 
        print_format->caption_start, 
        print_format->caption_col_div, print_format->caption_col_div, print_format->caption_col_div, print_format->caption_col_div,
        print_format->caption_col_div, print_format->caption_col_div, print_format->caption_col_div, print_format->caption_col_div,
        print_format->caption_col_div,
        print_format->caption_end
    );
}

void http_connection_get_stats_info_data (gpointer client, GString *str, struct PrintFormat *print_format)
{
    HttpConnection *con = (HttpConnection *) client;
    gchar cmd[10];
    char ts[50];
    guint diff_sec = 0;

    switch (con->cur_cmd_type) {
        case EVHTTP_REQ_GET:
            strcpy (cmd, "GET");
            break;
        case EVHTTP_REQ_PUT:
            strcpy (cmd, "PUT");
            break;
        case EVHTTP_REQ_POST:
            strcpy (cmd, "POST");
            break;
        case EVHTTP_REQ_DELETE:
            strcpy (cmd, "DELETE");
            break;
        case EVHTTP_REQ_HEAD:
            strcpy (cmd, "HEAD");
            break;
        default:
            strcpy (cmd, "-");
    }

    if (con->cur_time_start) {
        struct tm *cur_p;
        struct tm cur;
        
        localtime_r (&con->cur_time_start, &cur);
        cur_p = &cur;
        if (!strftime (ts, sizeof (ts), "%H:%M:%S", cur_p))
            ts[0] = '\0';
    } else {
        strcpy (ts, "");
    }

    if (con->cur_time_start && con->cur_time_start < con->cur_time_stop)
        diff_sec = con->cur_time_stop - con->cur_time_start;
    
    g_string_append_printf (str, 
        "%s" // header
        "%p %s" // ID
        "%s %s" // State
        "%s %s" // CMD
        "%s %s" // URL
        "%d %s" // Code
        "%s (%u sec) %s" // Time
        "%"G_GUINT64_FORMAT" (%"G_GUINT64_FORMAT") %s" // Jobs nr
        "%"G_GUINT64_FORMAT" %s" // Connects nr
        "~ %"G_GUINT64_FORMAT" b %s" // Total Out
        "~ %"G_GUINT64_FORMAT" b" // Total In
        "%s" // footer
        ,
        print_format->row_start,
        con, print_format->col_div,
        con->is_acquired ? "Busy" : "Idle", print_format->col_div,
        cmd, print_format->col_div,
        con->cur_url ? con->cur_url : "-",  print_format->col_div,
        con->cur_code, print_format->col_div,
        ts, diff_sec, print_format->col_div,
        con->jobs_nr, con->errors_nr, print_format->col_div,
        con->connects_nr, print_format->col_div,
        con->total_bytes_out, print_format->col_div,
        con->total_bytes_in,
        print_format->row_end
    );
}
