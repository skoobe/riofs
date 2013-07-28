/*
 * Copyright (C) 2012-2013 Paul Ionkin <paul.ionkin@gmail.com>
 * Copyright (C) 2012-2013 Skoobe GmbH. All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */
#ifndef _GLOBAL_H_
#define _GLOBAL_H_

#define _XOPEN_SOURCE
#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "config.h" 

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
#include <ftw.h>
#include <sys/xattr.h>

#include <glib.h>
#include <glib/gprintf.h>

#include <openssl/engine.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/md5.h>

#include <event2/event.h>
#include <event2/listener.h>
#include <event2/bufferevent.h>
#include <event2/bufferevent_struct.h>
#include <event2/buffer.h>
#include <event2/dns.h>
#include <event2/http.h>
#include <event2/http_struct.h>

#ifdef SSL_ENABLED
#include <event2/bufferevent_ssl.h>
#endif

#define HTTP_DEFAULT_PORT 80

#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <libxml/parser.h>
#include <libxml/tree.h>

//#define FUSE_USE_VERSION 26
#include <fuse/fuse_lowlevel.h>

typedef struct _Application Application;
typedef struct _HttpConnection HttpConnection;
typedef struct _DirTree DirTree;
typedef struct _RFuse RFuse;
typedef struct _ClientPool ClientPool;
typedef enum _LogLevel LogLevel;
typedef struct _ConfData ConfData;
typedef struct _CacheMng CacheMng;
typedef struct _StatSrv StatSrv;

struct event_base *application_get_evbase (Application *app);
struct evdns_base *application_get_dnsbase (Application *app);
ConfData *application_get_conf (Application *app);

ClientPool *application_get_read_client_pool (Application *app);
ClientPool *application_get_write_client_pool (Application *app);
ClientPool *application_get_ops_client_pool (Application *app);
DirTree *application_get_dir_tree (Application *app);
CacheMng *application_get_cache_mng (Application *app);
StatSrv *application_get_stat_srv (Application *app);
RFuse *application_get_rfuse (Application *app);

#ifdef SSL_ENABLED
SSL_CTX *application_get_ssl_ctx (Application *app);
#endif

// sets new S3 URL in case of redirect
gboolean application_set_url (Application *app, const gchar *url);

#include "log.h" 
#include "conf.h" 

struct PrintFormat {
    const gchar *header;
    const gchar *footer;
    const gchar *caption_start;
    const gchar *caption_end;
    const gchar *row_start;
    const gchar *row_end;
    const gchar *col_div;
    const gchar *caption_col_div;
};

LogLevel log_level;

#define FIVEG 5368709120  // five gigabytes

// XXX: improve this part !!
// this test is to determine Fuse ino_t size on 32bit systems
// tested on both 32bit and 64bit Ubuntu and Centos 
#ifdef SIZEOF_LONG_INT
    #if SIZEOF_LONG_INT == 4
        #define INO_FMT "lu"
        #define INO (unsigned long)
    #else 
        #define INO_FMT "llu"
        #define INO (unsigned long long)
    #endif
#else
    #define INO_FMT "lu"
    #define INO (unsigned long)
#endif

// we have defined -D_FILE_OFFSET_BITS=64
#define OFF_FMT G_GOFFSET_FORMAT

// log header to print INO
#define INO_H "[ino: %"INO_FMT"] "
#define INO_FOP_H "[ino: %"INO_FMT", fop: %p] "
#define INO_FI_H "[ino: %"INO_FMT", fi: %p] "
#define INO_FROP_H "[ino: %"INO_FMT", frop: %p] "
#define INO_T(x) (INO x)
#define CON_H "[con: %p] "
#define INO_CON_H "[ino: %"INO_FMT", con: %p] "

#endif
