#include "include/bucket_connection.h"

typedef struct {
    BucketConnection *con;
    const gchar *auth_str;
    gchar *resource_path;
    gchar *path;
} DirListRequest;

// read callback function
void bucket_connection_on_directory_listing (gpointer request, struct evbuffer *inbuf)
{
    struct evbuffer *buf;
    gchar *req_path;
    DirListRequest *dir_req = (DirListRequest *) request;
    
    if (inbuf) {
    }

    req_path = g_strdup_printf ("%s?delimiter=/&max-keys=%d&marker=&prefix=", dir_req->path, 2);

    buf = evbuffer_new ();
    buf = bucket_connection_append_headers (dir_req->con, buf, "GET", dir_req->auth_str, req_path);

    bucket_connection_send (dir_req->con, buf);
    
    g_free (req_path);
    evbuffer_free (buf);
}

// create DirListRequest
gboolean bucket_connection_get_directory_listing (BucketConnection *con, const gchar *path)
{
    DirListRequest *dir_req;
    S3Bucket *bucket;

    bucket = bucket_connection_get_bucket (con);

    dir_req = g_new0 (DirListRequest, 1);
    dir_req->con = con;
    dir_req->resource_path = g_strdup_printf ("/%s%s", bucket->name, path);
    dir_req->path = g_strdup (path);
    dir_req->auth_str = bucket_connection_get_auth_string (con, "GET", "", dir_req->resource_path);


    bucket_connection_set_request (con, dir_req);
    bucket_connection_on_directory_listing (dir_req, NULL);
    
    return TRUE;
}
