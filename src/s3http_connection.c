#include "include/s3http_connection.h"

/*{{{ struct*/

#define CON_LOG "con"

static void s3http_connection_on_close (struct evhttp_connection *evcon, void *ctx);
static const gchar *s3http_connection_get_auth_string (S3HttpConnection *con, 
        const gchar *method, const gchar *content_type, const gchar *resource, const gchar *time_str);

/*}}}*/

/*{{{ create / destroy */
// create S3HttpConnection object
// establish HTTP connections to S3
S3HttpConnection *s3http_connection_create (Application *app)
{
    S3HttpConnection *con;
    int port;

    con = g_new0 (S3HttpConnection, 1);
    if (!con) {
        LOG_err (CON_LOG, "Failed to create S3HttpConnection !");
        return NULL;
    }

    con->app = app;
    con->bucket_name = g_strdup (bucket_name);
    con->s3_uri = application_get_bucket_uri (app);

    port = evhttp_uri_get_port (con->s3_uri);
    // if no port is specified, libevent returns -1
    if (port == -1) {
        port = 80;
    }

    LOG_debug (CON_LOG, "Connecting to %s:%d", 
        evhttp_uri_get_host (con->s3_uri),
        port
    );

    // XXX: implement SSL
    con->evcon = evhttp_connection_base_bufferevent_new (
        application_get_evbase (app),
        application_get_dnsbase (app),
        NULL,
        evhttp_uri_get_host (con->s3_uri),
        port
    );

    if (!con->evcon) {
        LOG_err (CON_LOG, "Failed to create evhttp_connection !");
        return NULL;
    }
    
    // XXX: config these
    evhttp_connection_set_timeout (con->evcon, 20);
    evhttp_connection_set_retries (con->evcon, -1);

    evhttp_connection_set_closecb (con->evcon, s3http_connection_on_close, con);

    return con;
}

// destory S3HttpConnection
void s3http_connection_destroy (S3HttpConnection *con)
{
    evhttp_connection_free (con->evcon);
    g_free (con);
}
/*}}}*/

void s3http_connection_set_on_released_cb (S3HttpConnection *con, S3ClientPool_on_released_cb client_on_released_cb, gpointer ctx)
{
    con->client_on_released_cb = client_on_released_cb;
    con->pool_ctx = ctx;
}

// callback connection is closed
static void s3http_connection_on_close (struct evhttp_connection *evcon, void *ctx)
{
    S3HttpConnection *con = (S3HttpConnection *) ctx;

    LOG_debug (CON_LOG, "Connection closed !");
}

/*{{{ getters */
Application *s3http_connection_get_app (S3HttpConnection *con)
{
    return con->app;
}

struct evhttp_connection *s3http_connection_get_evcon (S3HttpConnection *con)
{
    return con->evcon;
}

/*}}}*/

/*{{{ get_auth_string */
// create S3 auth string
// http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/RESTAuthentication.html
static const gchar *s3http_connection_get_auth_string (S3HttpConnection *con, 
        const gchar *method, const gchar *content_type, const gchar *resource, const gchar *time_str)
{
    const gchar *string_to_sign;
    unsigned int md_len;
    unsigned char md[EVP_MAX_MD_SIZE];
    gchar *res;
    BIO *bmem, *b64;
    BUF_MEM *bptr;
    int ret;
    gchar *tmp;

    tmp = g_strdup_printf ("/%s%s", application_get_bucket_name (con->app), resource);

    string_to_sign = g_strdup_printf (
        "%s\n"  // HTTP-Verb + "\n"
        "%s\n"  // Content-MD5 + "\n"
        "%s\n"  // Content-Type + "\n"
        "%s\n"  // Date + "\n" 
        "%s"    // CanonicalizedAmzHeaders
        "%s",    // CanonicalizedResource

        method, "", content_type, time_str, "", tmp
    );

    g_free (tmp);

    LOG_debug (CON_LOG, "%s", string_to_sign);

    HMAC (EVP_sha1(), 
        application_get_secret_access_key (con->app),
        strlen (application_get_secret_access_key (con->app)),
        (unsigned char *)string_to_sign, strlen (string_to_sign),
        md, &md_len
    );
    
    b64 = BIO_new (BIO_f_base64 ());
    bmem = BIO_new (BIO_s_mem ());
    b64 = BIO_push (b64, bmem);
    BIO_write (b64, md, md_len);
    ret = BIO_flush (b64);
    if (ret != 1) {
        LOG_err (CON_LOG, "Failed to create base64 of auth string !");
        return NULL;
    }
    BIO_get_mem_ptr (b64, &bptr);

    res = g_malloc (bptr->length);
    memcpy (res, bptr->data, bptr->length);
    res[bptr->length - 1] = '\0';

    BIO_free_all (b64);

    return res;
}
/*}}}*/

// create S3 and setup HTTP connection request
struct evhttp_request *s3http_connection_create_request (S3HttpConnection *con,
    void (*cb)(struct evhttp_request *, void *), void *arg,
    const gchar *auth_str)
{    
    struct evhttp_request *req;
    gchar auth_key[300];
    struct tm *cur_p;
	time_t t = time(NULL);
    struct tm cur;
    char date[50];
    //char hostname[1024];

	gmtime_r(&t, &cur);
	cur_p = &cur;

    snprintf (auth_key, sizeof (auth_key), "AWS %s:%s", application_get_access_key_id (con->app), auth_str);

    req = evhttp_request_new (cb, arg);
    evhttp_add_header (req->output_headers, 
        "Authorization", auth_key);
    evhttp_add_header (req->output_headers, 
        "Host", application_get_bucket_url (con->app));
		
    if (strftime(date, sizeof(date), "%a, %d %b %Y %H:%M:%S GMT", cur_p) != 0) {
			evhttp_add_header (req->output_headers, "Date", date);
		}
    return req;
}


typedef struct {
    S3HttpConnection_responce_cb responce_cb;
    S3HttpConnection_error_cb error_cb;
    gpointer ctx;
} RequestData;

static void s3http_connection_on_responce_cb (struct evhttp_request *req, void *ctx)
{
    RequestData *data = (RequestData *) ctx;
    struct evbuffer *inbuf;
    const char *buf;
    size_t buf_len;

    if (!req) {
        LOG_err (CON_LOG, "Request failed !");
        if (data->error_cb)
            data->error_cb (data->ctx);
        goto done;
    }

    // XXX: handle redirect
    // 200 and 204 (No Content) are ok
    if (evhttp_request_get_response_code (req) != 200 && evhttp_request_get_response_code (req) != 204) {
        LOG_err (CON_LOG, "Server returned HTTP error: %d !", evhttp_request_get_response_code (req));
        LOG_debug (CON_LOG, "Error str: %s", req->response_code_line);
        if (data->error_cb)
            data->error_cb (data->ctx);
        goto done;
    }

    inbuf = evhttp_request_get_input_buffer (req);
    buf_len = evbuffer_get_length (inbuf);
    buf = (const char *) evbuffer_pullup (inbuf, buf_len);
    
    if (data->responce_cb)
        data->responce_cb (data->ctx, buf, buf_len);

done:
    g_free (data);
}

gboolean s3http_connection_make_request (S3HttpConnection *con, 
    const gchar *resource_path, const gchar *request_str,
    const gchar *http_cmd,
    struct evbuffer *out_buffer,
    S3HttpConnection_responce_cb responce_cb,
    S3HttpConnection_error_cb error_cb,
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
    
    data = g_new0 (RequestData, 1);
    data->responce_cb = responce_cb;
    data->error_cb = error_cb;
    data->ctx = ctx;
    
    if (!strcasecmp (http_cmd, "GET")) {
        cmd_type = EVHTTP_REQ_GET;
    } else if (!strcasecmp (http_cmd, "PUT")) {
        cmd_type = EVHTTP_REQ_PUT;
    } else if (!strcasecmp (http_cmd, "DELETE")) {
        cmd_type = EVHTTP_REQ_DELETE;
    } else {
        LOG_err (CON_LOG, "Unsupported HTTP method: %s", http_cmd);
        return FALSE;
    }
    
    t = time (NULL);
    strftime (time_str, sizeof (time_str), "%a, %d %b %Y %H:%M:%S GMT", gmtime(&t));
    auth_str = s3http_connection_get_auth_string (con, http_cmd, "", resource_path, time_str);
    snprintf (auth_key, sizeof (auth_key), "AWS %s:%s", application_get_access_key_id (con->app), auth_str);

    req = evhttp_request_new (s3http_connection_on_responce_cb, data);
    if (!req) {
        LOG_err (CON_LOG, "Failed to create HTTP request object !");
        return FALSE;
    }

    evhttp_add_header (req->output_headers, "Authorization", auth_key);
    evhttp_add_header (req->output_headers, "Host", application_get_bucket_url (con->app));	
	evhttp_add_header (req->output_headers, "Date", time_str);

    if (out_buffer) {
        evbuffer_add_buffer (req->output_buffer, out_buffer);
    }

    res = evhttp_make_request (s3http_connection_get_evcon (con), req, cmd_type, request_str);

    if (res < 0)
        return FALSE;
    else
        return TRUE;
}    
