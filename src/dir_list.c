#include "include/bucket_connection.h"

typedef struct {
    BucketConnection *con;
    const gchar *auth_str;
    gchar *resource_path;
    gchar *path;
} DirListRequest;

// read callback function
static void bucket_connection_on_directory_listing (struct evhttp_request *req, void *ctx)
{   
    DirListRequest *dir_req = (DirListRequest *) ctx;
    struct evbuffer *inbuf;

    inbuf = evhttp_request_get_input_buffer (req);
    g_printf ("======================\n%s\n=======================\n", evbuffer_pullup (inbuf, -1));
}

// create DirListRequest
gboolean bucket_connection_get_directory_listing (BucketConnection *con, const gchar *path)
{
    DirListRequest *dir_req;
    S3Bucket *bucket;
    struct evhttp_request *req;
    gchar *req_path;
    int res;

    bucket = bucket_connection_get_bucket (con);

    dir_req = g_new0 (DirListRequest, 1);
    dir_req->con = con;
    dir_req->resource_path = g_strdup_printf ("/%s%s", bucket->name, path);
    dir_req->path = g_strdup (path);
    dir_req->auth_str = bucket_connection_get_auth_string (con, "GET", "", dir_req->resource_path);

    req_path = g_strdup_printf ("%s?delimiter=/&max-keys=%d&marker=&prefix=", dir_req->path, 1000);


    req = bucket_connection_create_request (con, bucket_connection_on_directory_listing, dir_req, dir_req->auth_str);
    res = evhttp_make_request (bucket_connection_get_evcon (con), req, EVHTTP_REQ_GET, req_path);
    g_printf ("Res: %d\n", res);
    //buf = bucket_connection_append_headers (dir_req->con, buf, "GET", dir_req->auth_str, req_path);

    return TRUE;
}
