#include "include/s3http_connection.h"

/*{{{ struct*/

#define CON_LOG "con"

static void s3http_connection_on_close (struct evhttp_connection *evcon, void *ctx);
/*}}}*/

/*{{{ create / destroy */
// create S3HttpConnection object
// establish HTTP connections to S3
S3HttpConnection *s3http_connection_create (Application *app, struct evhttp_uri *s3_url, const gchar *bucket_name)
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
    con->s3_url = s3_url;

    port = evhttp_uri_get_port (s3_url);
    // if no port is specified, libevent returns -1
    if (port == -1) {
        port = 80;
    }

    LOG_debug (CON_LOG, "Connecting to %s:%d", 
        evhttp_uri_get_host (s3_url),
        port
    );

    // XXX: implement SSL
    con->evcon = evhttp_connection_base_bufferevent_new (
        application_get_evbase (app),
        application_get_dnsbase (app),
        NULL,
        evhttp_uri_get_host (s3_url),
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
const gchar *s3http_connection_get_auth_string (S3HttpConnection *con, 
        const gchar *method, const gchar *content_type, const gchar *resource)
{
    const gchar *string_to_sign;
    char time_str[100];
    time_t t = time (NULL);
    unsigned int md_len;
    unsigned char md[EVP_MAX_MD_SIZE];
    gchar *res;
    BIO *bmem, *b64;
    BUF_MEM *bptr;
    int ret;

    strftime (time_str, sizeof (time_str), "%a, %d %b %Y %H:%M:%S GMT", gmtime(&t));

    string_to_sign = g_strdup_printf (
        "%s\n"  // HTTP-Verb + "\n"
        "%s\n"  // Content-MD5 + "\n"
        "%s\n"  // Content-Type + "\n"
        "%s\n"  // Date + "\n" 
        "%s"    // CanonicalizedAmzHeaders
        "%s",    // CanonicalizedResource

        method, "", content_type, time_str, "", resource
    );

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

	gmtime_r(&t, &cur);
	cur_p = &cur;

    snprintf (auth_key, sizeof (auth_key), "AWS %s:%s", application_get_access_key_id (con->app), auth_str);

    req = evhttp_request_new (cb, arg);
    evhttp_add_header (req->output_headers, 
        "Authorization", auth_key);
    evhttp_add_header (req->output_headers, 
        "Host", evhttp_uri_get_host (con->s3_url));
		
    if (strftime(date, sizeof(date), "%a, %d %b %Y %H:%M:%S GMT", cur_p) != 0) {
			evhttp_add_header (req->output_headers, "Date", date);
		}
    return req;
}
