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
#include "cache_mng.h"
#include "range.h"

struct _CacheMng {
    Application *app;
    ConfData *conf;
    GHashTable *h_entries; 
};

struct _CacheEntry {
    fuse_ino_t ino;
    Range *avail_range;
};

enum _CacheOp {
    CACHE_OP_RETRIEVE,
    CACHE_OP_STORE
};

struct _CacheContext {
    CacheMng *cmng;
    enum _CacheOp op;
    fuse_ino_t ino;
    size_t size;
    off_t off;
    unsigned char *buf;
    gboolean success;
    union {
        cache_mng_on_retrieve_file_buf_cb retrieve_cb;
        cache_mng_on_store_file_buf_cb store_cb;
    } cb;
    void *user_ctx;
};

static void cache_entry_destroy(gpointer data);

CacheMng *cache_mng_create (Application *app)
{
    CacheMng *cmng;

    cmng = g_new0 (CacheMng, 1);
    cmng->app = app;
    cmng->conf = application_get_conf (app);
    cmng->h_entries = g_hash_table_new_full (g_direct_hash, g_direct_equal, NULL, cache_entry_destroy);

    return cmng;
}

void cache_mng_destroy (CacheMng *cmng)
{
    g_hash_table_destroy (cmng->h_entries);
    g_free (cmng);
}

static struct _CacheEntry* cache_entry_create ()
{
    struct _CacheEntry* entry = g_malloc (sizeof (struct _CacheEntry));

    entry->avail_range = range_create ();

    return entry;
}

static void cache_entry_destroy (gpointer data)
{
    struct _CacheEntry * entry = (struct _CacheEntry*) data;

    range_destroy(entry->avail_range);
    g_free(entry);
}

static struct _CacheContext* cache_context_create (CacheMng *cmng,
                                                   fuse_ino_t ino,
                                                   enum _CacheOp op, size_t size,
                                                   off_t off, unsigned char *buf,
                                                   void *user_ctx)
{
    struct _CacheContext *context = g_malloc (sizeof (struct _CacheContext));

    context->cmng = cmng;
    context->ino = ino;
    context->op = op;
    context->off = off;
    context->size = size;
    context->user_ctx = user_ctx;
    context->success = FALSE;
    if (buf) {
        context->buf = g_malloc (size);
        memcpy (context->buf, buf, size);
    } else {
        context->buf = NULL;
    }

    return context;
}

static void cache_context_destroy (struct _CacheContext* context)
{
    g_free (context->buf);
    g_free (context);
}

static void cache_read_cb (evutil_socket_t fd, short flags, void *ctx)
{
    struct _CacheContext *context = (struct _CacheContext *) ctx;

    context->cb.retrieve_cb (context->buf, context->size, context->success, context->user_ctx);
    cache_context_destroy (context);
}

static int cache_mng_file_name (CacheMng *cmng, char *buf, int buflen, fuse_ino_t ino)
{
    return snprintf (buf, buflen, "cache_mng_%"INO_FMT"", ino);
}

// retrieve file buffer from local storage
// if success == TRUE then "buf" contains "size" bytes of data
void cache_mng_retrieve_file_buf (CacheMng *cmng, fuse_ino_t ino, size_t size, off_t off, 
    cache_mng_on_retrieve_file_buf_cb on_retrieve_file_buf_cb, void *ctx)
{
    struct _CacheContext *context;
    struct _CacheEntry *entry;
    struct event *ev;

    context = cache_context_create (cmng, ino, CACHE_OP_RETRIEVE, size, off, NULL, ctx);
    context->cb.retrieve_cb = on_retrieve_file_buf_cb;
    entry = g_hash_table_lookup (cmng->h_entries, GUINT_TO_POINTER (ino));

    if (entry && range_contain (entry->avail_range, off, off + size)) {
        int fd;
        ssize_t res;
        char path[PATH_MAX];

        cache_mng_file_name (cmng, path, sizeof (path), ino);
        fd = open (path, O_RDONLY);

        context->buf = g_malloc (size);
        res = pread (fd, context->buf, size, off);
        close (fd);
        context->success = (res == size);
        if (!context->success) {
            g_free (context->buf);
            context->buf = NULL;
        }
    }

    ev = event_new (application_get_evbase (cmng->app), -1,  0,
                    cache_read_cb, context);
    event_active (ev, 0, 0);
    event_add (ev, NULL);
}

static void cache_write_cb (evutil_socket_t fd, short flags, void *ctx)
{
    struct _CacheContext *context = (struct _CacheContext *) ctx;

    context->cb.store_cb (context->success, context->user_ctx);

    cache_context_destroy (context);
}

// store file buffer into local storage
// if success == TRUE then "buf" successfuly stored on disc
void cache_mng_store_file_buf (CacheMng *cmng, fuse_ino_t ino, size_t size, off_t off, unsigned char *buf,
    cache_mng_on_store_file_buf_cb on_store_file_buf_cb, void *ctx)
{
    struct _CacheContext *context;
    struct _CacheEntry *entry;
    ssize_t res;
    int fd;
    char path[PATH_MAX];
    struct event *ev;

    context = cache_context_create (cmng, ino, CACHE_OP_STORE, size, off, NULL, ctx);
    context->cb.store_cb = on_store_file_buf_cb;

    cache_mng_file_name (cmng, path, sizeof (path), ino);
    fd = open (path, O_WRONLY|O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    res = pwrite(fd, buf, size, off);
    close (fd);

    entry = g_hash_table_lookup (cmng->h_entries, GUINT_TO_POINTER (ino));

    if (!entry) {
        entry = cache_entry_create ();
        g_hash_table_insert (context->cmng->h_entries, GUINT_TO_POINTER (ino), entry);
    }

    range_add (entry->avail_range, off, off + size);

    context->success = (res == size);

    ev = event_new (application_get_evbase (cmng->app), -1,  0,
                    cache_write_cb, context);
    event_active (ev, 0, 0);
    event_add (ev, NULL);
}

// removes file from local storage
void cache_mng_remove_file (CacheMng *cmng, fuse_ino_t ino)
{
    struct _CacheEntry *entry;
    char path[PATH_MAX];

    if (g_hash_table_remove (cmng->h_entries, GUINT_TO_POINTER (ino))) {
        cache_mng_file_name (cmng, path, sizeof (path), ino);
        unlink (path);
    }
}
