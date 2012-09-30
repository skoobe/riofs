#ifndef _S3_CLIENT_POOL_H_
#define _S3_CLIENT_POOL_H_

#include "include/global.h"

typedef void (*S3ClientPool_client_create) (Application *app);
typedef void (*S3ClientPool_on_released_cb) (gpointer client, gpointer ctx);
typedef void (*S3ClientPool_client_set_on_released_cb) (gpointer client, S3ClientPool_on_released_cb client_on_released_cb, gpointer ctx);

S3ClientPool *s3client_pool_create (Application *app, 
    gint client_count,
    S3ClientPool_client_create client_create, 
    S3ClientPool_client_set_on_released_cb client_set_on_released_cb);    

void s3client_pool_destroy (S3ClientPool *pool);

// add client's callback to the awaiting queue
// return TRUE if added, FALSE if list is full


#endif
