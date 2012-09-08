
#include "include/bucket_connection.h"

struct _BucketConnection {
    Application *app;
    S3Bucket *bucket;

    struct evhttp_connection *evcon;
};

static void bucket_connection_on_close (struct evhttp_connection *evcon, void *ctx);

// creates BucketConnection object
// establishes HTTP connections to S3
BucketConnection *bucket_connection_new (Application *app, S3Bucket *bucket)
{
    BucketConnection *con;
    int port;

    con = g_new0 (BucketConnection, 1);
    if (!con) {
        LOG_err ("Failed to create BucketConnection !");
        return NULL;
    }

    con->app = app;
    con->bucket = bucket;
    
    port = evhttp_uri_get_port (con->bucket->uri);
    // if no port is specified, libevent returns -1. Set it to default on
    if (port == -1) {
        port = 80;
    }

    LOG_debug ("Connecting to %s:%d", 
        evhttp_uri_get_host (con->bucket->uri),
        port
    );

    // XXX: implement SSL
    con->evcon = evhttp_connection_base_bufferevent_new (
        application_get_evbase (app),
        application_get_dnsbase (app),
        NULL,
        evhttp_uri_get_host (con->bucket->uri),
        port
    );

    if (!con->evcon) {
        LOG_err ("Failed to create evhttp_connection !");
        return NULL;
    }
    
    // XXX: config these
    evhttp_connection_set_timeout (con->evcon, 60);
    evhttp_connection_set_retries (con->evcon, -1);

    evhttp_connection_set_closecb (con->evcon, bucket_connection_on_close, con);

    return con;
}

// destory BucketConnection
void bucket_connection_destroy (BucketConnection *con)
{
    evhttp_connection_free (con->evcon);
    g_free (con);
}

// connection is closed
static void bucket_connection_on_close (struct evhttp_connection *evcon, void *ctx)
{
    BucketConnection *con = (BucketConnection *) ctx;

    LOG_debug ("Connection closed !");
}


S3Bucket *bucket_connection_get_bucket (BucketConnection *con)
{
    return con->bucket;
}

Application *bucket_connection_get_app (BucketConnection *con)
{
    return con->app;
}

struct evhttp_connection *bucket_connection_get_evcon (BucketConnection *con)
{
    return con->evcon;
}

// create S3 auth string
const gchar *bucket_connection_get_auth_string (BucketConnection *con, 
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
        LOG_err ("Failed to create base64 of auth string !");
        return NULL;
    }
    BIO_get_mem_ptr (b64, &bptr);

    res = g_malloc (bptr->length);
    memcpy (res, bptr->data, bptr->length);
    res[bptr->length - 1] = '\0';

    BIO_free_all (b64);

    return res;
}

// create S3 connection request
struct evhttp_request *bucket_connection_create_request (BucketConnection *con,
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
        "Host", evhttp_uri_get_host (con->bucket->uri));
		
    if (strftime(date, sizeof(date), "%a, %d %b %Y %H:%M:%S GMT", cur_p) != 0) {
			evhttp_add_header (req->output_headers, "Date", date);
		}
    return req;
}
