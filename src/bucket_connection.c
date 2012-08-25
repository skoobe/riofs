#include "include/bucket_connection.h"
#include "include/utils.h"

struct _BucketConnection {
    Application *app;
    S3Bucket *bucket;

    struct bufferevent *bev;

    // only 1 request per connection
    gpointer request;
    RequestType req_type;
};

static void bucket_connection_on_read (struct bufferevent *bev, void *ctx);
static void bucket_connection_on_write (struct bufferevent *bev, void *ctx);
static void bucket_connection_on_even (struct bufferevent *bev, short what, void *ctx);
static void bucket_connection_on_connection (BucketConnection *con);

#define READ_TIMEOUT 20
#define WRITE_TIMEOUT 20

// creates BucketConnection object
BucketConnection *bucket_connection_new (Application *app, S3Bucket *bucket)
{
    BucketConnection *con;
    struct timeval timeout_read;
    struct timeval timeout_write;

    con = g_new0 (BucketConnection, 1);
    if (!con) {
        LOG_err ("Failed to create BucketConnection !");
        return NULL;
    }

    con->app = app;
    con->bucket = bucket;
    
    // create bufferevent
    con->bev = bufferevent_socket_new (application_get_evbase (app), -1, BEV_OPT_CLOSE_ON_FREE);
    if (!con->bev) {
        LOG_err ("Failed to create bufferevent !");
        return NULL;
    }

    // set callbacks
    bufferevent_setcb (con->bev, 
        bucket_connection_on_read,
        bucket_connection_on_write,
        bucket_connection_on_even,
        con
    );

    // set timeouts
    timeout_read.tv_usec = 0;
    timeout_read.tv_sec = READ_TIMEOUT;
    timeout_write.tv_usec = 0;
    timeout_write.tv_sec = WRITE_TIMEOUT;
    bufferevent_set_timeouts (con->bev, &timeout_read, &timeout_write);
    
    // enable events
    bufferevent_enable (con->bev, EV_READ | EV_WRITE);

    return con;
}

// destory BucketConnection
void bucket_connection_destroy (BucketConnection *con)
{
    bufferevent_free (con->bev);
    g_free (con);
}

S3Bucket *bucket_connection_get_bucket (BucketConnection *con)
{
    return con->bucket;
}

void bucket_connection_set_request (BucketConnection *con, gpointer request)
{
    con->request = request;
}

// Received data
static void bucket_connection_on_read (struct bufferevent *bev, void *ctx)
{
    BucketConnection *con = (BucketConnection *)ctx;
    struct evbuffer *inbuf;

    inbuf = bufferevent_get_input (bev);

    LOG_debug ("Received %zd bytes", evbuffer_get_length (inbuf));
    g_printf ("========================\n%s\n===================\n", evbuffer_pullup (inbuf,  -1));

    // XXX: read headers
    //
    // if ok:
    switch (con->req_type) {
        case RT_list: 
            bucket_connection_on_directory_listing (con->request, inbuf);
            break;
        default:
            break;
    }
}

// Sending data callback
static void bucket_connection_on_write (struct bufferevent *bev, void *ctx)
{
    BucketConnection *con = (BucketConnection *)ctx;

    struct evbuffer *outbuf;

    outbuf = bufferevent_get_output (bev);

   // LOG_debug ("Sending %zd bytes", evbuffer_get_length (outbuf));
}

// sends data
void bucket_connection_send (BucketConnection *con, struct evbuffer *outbuf)
{
    LOG_debug ("Sending %zd bytes:", evbuffer_get_length (outbuf));
    g_printf ("========================\n%s\n===================\n", evbuffer_pullup (outbuf, -1));
    bufferevent_write_buffer (con->bev, outbuf);
}

// Connection event
static void bucket_connection_on_even (struct bufferevent *bev, short what, void *ctx)
{
    BucketConnection *con = (BucketConnection *)ctx;
    
    if (what & BEV_EVENT_EOF) {
        LOG_debug ("BEV_EVENT_EOF");
    } else if (what & BEV_EVENT_ERROR) {
        LOG_debug ("BEV_EVENT_ERROR"); 
    } else if (what & BEV_EVENT_TIMEOUT) {
        if (what & BEV_EVENT_READING)
            LOG_debug ("BEV_EVENT_TIMEOUT reading");
        else if (what & BEV_EVENT_WRITING)
            LOG_debug ("BEV_EVENT_TIMEOUT writing");
        else
            LOG_debug ("BEV_EVENT_TIMEOUT");
    } else if (what & BEV_EVENT_CONNECTED) {
        LOG_debug ("BEV_EVENT_CONNECTED");
        bucket_connection_on_connection (con);
    } else {
        LOG_debug ("Other connection error");
    }

  //  net_client_disconnect (tcp_client->net_client);
}


// establishes connection to S3
// Returns TRUE if Ok
gboolean bucket_connection_connect (BucketConnection *con)
{
    int res;
    int port;

    port = evhttp_uri_get_port (con->bucket->uri);
    // if no port is specified, libevent returns -1. Set it to default on
    if (port == -1) {
        port = 80;
    }

    LOG_debug ("Connecting to %s:%d", 
        evhttp_uri_get_host (con->bucket->uri),
        port
    );

    res = bufferevent_socket_connect_hostname (
        con->bev, 
        application_get_dnsbase (con->app), 
        AF_INET, 
        evhttp_uri_get_host (con->bucket->uri),
        port
    );

    if (res) {
        LOG_err ("Failed to establish connection to %s:%d  (code: %d)", 
            evhttp_uri_get_host (con->bucket->uri), port,
            bufferevent_socket_get_dns_error (con->bev)
        );

        return FALSE;
    }

    return TRUE;
}

// connection established 
static void bucket_connection_on_connection (BucketConnection *con)
{
    LOG_debug ("Connection established !");
    application_connected (con->app, con);
}


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

    LOG_debug ("%s", string_to_sign);

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

    res = g_malloc (bptr->length + 1);
    memcpy (res, bptr->data, bptr->length);
    res[bptr->length] = '\0';

    BIO_free_all (b64);

    return res;
}

struct evbuffer *bucket_connection_append_headers (BucketConnection *con, struct evbuffer *buf,
    const gchar *method, const gchar *auth_str, const gchar *request_path)
{    
    const gchar *host;
    char time_str[100];
    time_t t = time (NULL);

    host = evhttp_uri_get_host (con->bucket->uri);
    strftime (time_str, sizeof (time_str), "%a, %d %b %Y %H:%M:%S GMT", gmtime(&t));
    
    evbuffer_add_printf (buf, "%s %s HTTP/1.1\n", method, request_path);
    evbuffer_add_printf (buf, "Host: %s\n", host);
    evbuffer_add_printf (buf, "Date: %s\n", time_str);
    evbuffer_add_printf (buf, "Authorization: AWS %s:%s\n", application_get_access_key_id (con->app), auth_str);
    evbuffer_add_printf (buf, "\n");

    return buf;
}
