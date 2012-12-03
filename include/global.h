/*
 * Copyright (C) 2012  Paul Ionkin <paul.ionkin@gmail.com>
 * Copyright (C) 2012 Skoobe GmbH. All rights reserved.
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
typedef struct _S3HttpConnection S3HttpConnection;
typedef struct _DirTree DirTree;
typedef struct _S3Fuse S3Fuse;
typedef struct _S3ClientPool S3ClientPool;
typedef enum _LogLevel LogLevel;

struct event_base *application_get_evbase (Application *app);
struct evdns_base *application_get_dnsbase (Application *app);
const gchar *application_get_access_key_id (Application *app);
const gchar *application_get_secret_access_key (Application *app);
const gchar *application_get_bucket_uri_str (Application *app);
struct evhttp_uri *application_get_bucket_uri (Application *app);
const gchar *application_get_bucket_name (Application *app);
const gchar *application_get_tmp_dir (Application *app);
S3ClientPool *application_get_read_client_pool (Application *app);
S3ClientPool *application_get_write_client_pool (Application *app);
S3ClientPool *application_get_ops_client_pool (Application *app);
DirTree *application_get_dir_tree (Application *app);

#include "log.h" 

LogLevel log_level;

#define OFF_FMT "ju"
#define INO_FMT "llu"

#endif
