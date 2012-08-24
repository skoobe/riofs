#ifndef _S3_H_
#define _S3_H_

#include "include/global.h"

typedef struct {
    // low level stuff
    evutil_socket_t fd;

    const gchar *host;
    struct evhttp_uri *uri;
} S3Bucket;

#endif
