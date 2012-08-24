#include "include/global.h"

BucketConnection *bucket_connection_new (Application *app, S3Bucket *bucket);
void bucket_connection_destroy (BucketConnection *con);

gboolean bucket_connection_connect (BucketConnection *con);

gboolean bucket_connection_get_directory_listing (BucketConnection *con, const gchar *path);
