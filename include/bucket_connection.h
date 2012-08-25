#include "include/global.h"

typedef enum {
    RT_list = 0,
} RequestType;


BucketConnection *bucket_connection_new (Application *app, S3Bucket *bucket);
void bucket_connection_destroy (BucketConnection *con);

S3Bucket *bucket_connection_get_bucket (BucketConnection *con);
void bucket_connection_set_request (BucketConnection *con, gpointer request);

gboolean bucket_connection_connect (BucketConnection *con);

void bucket_connection_send (BucketConnection *con, struct evbuffer *outbuf);


const gchar *bucket_connection_get_auth_string (BucketConnection *con, 
        const gchar *method, const gchar *content_type, const gchar *resource);
struct evbuffer *bucket_connection_append_headers (BucketConnection *con, struct evbuffer *buf,
    const gchar *method, const gchar *auth_str, const gchar *request_path);


gboolean bucket_connection_get_directory_listing (BucketConnection *con, const gchar *path);
void bucket_connection_on_directory_listing (gpointer request, struct evbuffer *inbuf);
