#include "include/bucket_connection.h"

struct _BucketConnection {
    Application *app;
    S3Bucket *bucket;

    struct bufferevent *bev;
};

static void bucket_connection_on_read (struct bufferevent *bev, void *ctx);
static void bucket_connection_on_write (struct bufferevent *bev, void *ctx);
static void bucket_connection_on_even (struct bufferevent *bev, short what, void *ctx);

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

    return con;
}

// destory BucketConnection
void bucket_connection_destroy (BucketConnection *con)
{
    bufferevent_free (con->bev);
    g_free (con);
}

// Received data
static void bucket_connection_on_read (struct bufferevent *bev, void *ctx)
{
    BucketConnection *con = (BucketConnection *)ctx;
    struct evbuffer *inbuf;

    inbuf = bufferevent_get_input (bev);

    LOG_debug ("Received %zd bytes", evbuffer_get_length (inbuf));
}

// Sending data
static void bucket_connection_on_write (struct bufferevent *bev, void *ctx)
{
    BucketConnection *con = (BucketConnection *)ctx;
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
void bucket_connection_on_connection (BucketConnection *con)
{
    LOG_debug ("Connection established !");
}


const gchar *bucket_connection_get_auth_string (BucketConnection *con, 
        const gchar *method, const gchar *content_type, const gchar *resource)
{
    const gchar *string_to_sign;
    char time_str[100];
    time_t t = time (NULL);

    strftime (time_str, sizeof (time_str), "%a, %d %b %Y %H:%M:%S GMT", gmtime(&t));


    string_to_sign = g_strdup_printf (
        "%s\n"  // HTTP-Verb + "\n"
        "%s\n"  // Content-MD5 + "\n"
        "%s\n"  // Content-Type + "\n"
        "%s\n"  // Date + "\n" 
        "%s"    // CanonicalizedAmzHeaders
        "%s",    // CanonicalizedResource
    );    

}

gboolean bucket_connection_get_directory_listing (BucketConnection *con, const gchar *path)
{
}
