#ifndef _S3_HTTP_CLIENT_POOL_H_
#define _S3_HTTP_CLIENT_POOL_H_

#include "include/global.h"
#include "include/s3http_client.h"

typedef struct _S3HttpClientPool S3HttpClientPool;

S3HttpClientPool *s3http_client_pool_create (struct event_base *evbase, struct evdns_base *dns_base, gint client_count);
void s3http_client_pool_destroy (S3HttpClientPool *pool);

typedef void (*S3HttpClientPool_on_get) (S3HttpClient *http, gpointer pool_ctx);
// add client's callback to the awaiting queue
// return TRUE if added, FALSE if list is full
gboolean s3http_client_pool_get_S3HttpClient (S3HttpClientPool *pool, S3HttpClientPool_on_get on_get, gpointer ctx);


#endif
