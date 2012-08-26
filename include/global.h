#ifndef _GLOBAL_H_
#define _GLOBAL_H_

#define _XOPEN_SOURCE
#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <endian.h>
#include <gnu/libc-version.h>
#include <execinfo.h>
#include <signal.h>
#include <sys/queue.h>
#include <ctype.h>
#include <sys/types.h>
#include <pwd.h>
#include <sys/resource.h>
#include <errno.h>
#include <sys/prctl.h>
#include <netinet/tcp.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <math.h>

#include <glib.h>
#include <glib/gprintf.h>

#include <openssl/engine.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>

#include <event2/event.h>
#include <event2/listener.h>
#include <event2/bufferevent.h>
#include <event2/bufferevent_struct.h>
#include <event2/buffer.h>
#include <event2/dns.h>
#include <event2/http.h>
#include <event2/http_struct.h>

#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <libxml/parser.h>
#include <libxml/tree.h>

#define FUSE_USE_VERSION 26

#include <fuse/fuse_lowlevel.h>

typedef struct _Application Application;
typedef struct _BucketConnection BucketConnection;
typedef struct _DirTree DirTree;
typedef struct _FuseInfo FuseInfo;

struct event_base *application_get_evbase (Application *app);
struct evdns_base *application_get_dnsbase (Application *app);
DirTree *application_get_dir_tree (Application *app);
const gchar *application_get_access_key_id (Application *app);
const gchar *application_get_secret_access_key (Application *app);
void application_connected (Application *app, BucketConnection *con);

#include "include/log.h" 
#include "include/s3.h" 

#endif
