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
#include "utils.h"
#include "conf.h"

#define CACHE_MNGR_DIR "riofs_cache"
#define CMNG_LOG "cmng"

struct _CacheMng {
    Application *app;
    ConfData *conf;
    GHashTable *h_entries; 
    GQueue *q_lru;
    guint64 size;
    guint64 max_size;
    gchar *cache_dir;
};

struct _CacheEntry {
    fuse_ino_t ino;
    Range *avail_range;
    time_t modification_time;
    GList *ll_lru;
};

struct _CacheContext {
    guint64 size;
    unsigned char *buf;
    gboolean success;
    union {
        cache_mng_on_retrieve_file_buf_cb retrieve_cb;
        cache_mng_on_store_file_buf_cb store_cb;
    } cb;
    void *user_ctx;
    struct event *ev;
};

static void cache_entry_destroy (gpointer data);
static void cache_mng_rm_cache_dir (CacheMng *cmng);

CacheMng *cache_mng_create (Application *app)
{
    CacheMng *cmng;

    cmng = g_new0 (CacheMng, 1);
    cmng->app = app;
    cmng->conf = application_get_conf (app);
    cmng->h_entries = g_hash_table_new_full (g_direct_hash, g_direct_equal, NULL, cache_entry_destroy);
    cmng->q_lru = g_queue_new ();
    cmng->size = 0;
    cmng->max_size = conf_get_uint (cmng->conf, "filesystem.cache_dir_max_size");
    cmng->cache_dir = g_strdup_printf ("%s/%s", conf_get_string (cmng->conf, "filesystem.cache_dir"), CACHE_MNGR_DIR);

    cache_mng_rm_cache_dir (cmng);
    if (g_mkdir_with_parents (cmng->cache_dir, 0700) != 0) {
        LOG_err (CMNG_LOG, "Failed to remove directory: %s", cmng->cache_dir);
        cache_mng_destroy (cmng);
        return NULL;
    }

    return cmng;
}

void cache_mng_destroy (CacheMng *cmng)
{
    cache_mng_rm_cache_dir (cmng);
    g_free (cmng->cache_dir);
    g_queue_free (cmng->q_lru);
    g_hash_table_destroy (cmng->h_entries);
    g_free (cmng);
}

static struct _CacheEntry* cache_entry_create (fuse_ino_t ino)
{
    struct _CacheEntry* entry = g_malloc (sizeof (struct _CacheEntry));

    entry->ino = ino;
    entry->avail_range = range_create ();
    entry->ll_lru = NULL;
    entry->modification_time = time (NULL);

    return entry;
}

static void cache_entry_destroy (gpointer data)
{
    struct _CacheEntry * entry = (struct _CacheEntry*) data;

    range_destroy(entry->avail_range);
    g_free(entry);
}

static struct _CacheContext* cache_context_create (guint64 size, void *user_ctx)
{
    struct _CacheContext *context = g_malloc (sizeof (struct _CacheContext));

    context->user_ctx = user_ctx;
    context->success = FALSE;
    context->size = size;
    context->buf = NULL;
    context->ev = NULL;

    return context;
}

static void cache_mng_rm_cache_dir (CacheMng *cmng)
{
    if (cmng->cache_dir && strstr (cmng->cache_dir, CACHE_MNGR_DIR))
        utils_del_tree (cmng->cache_dir, 1);
    else {
        LOG_err (CMNG_LOG, "Cache directory not found: %s", cmng->cache_dir);
    }
}

static void cache_context_destroy (struct _CacheContext* context)
{
    event_free (context->ev);
    g_free (context->buf);
    g_free (context);
}

static void cache_read_cb (G_GNUC_UNUSED evutil_socket_t fd, G_GNUC_UNUSED short flags, void *ctx)
{
    struct _CacheContext *context = (struct _CacheContext *) ctx;

    if (context->cb.retrieve_cb)
        context->cb.retrieve_cb (context->buf, context->size, context->success, context->user_ctx);
    cache_context_destroy (context);
}

static int cache_mng_file_name (CacheMng *cmng, char *buf, int buflen, fuse_ino_t ino)
{
    return snprintf (buf, buflen, "%s/cache_mng_%"INO_FMT"", cmng->cache_dir, INO ino);
}

// retrieve file buffer from local storage
// if success == TRUE then "buf" contains "size" bytes of data
void cache_mng_retrieve_file_buf (CacheMng *cmng, fuse_ino_t ino, size_t size, off_t off, 
    cache_mng_on_retrieve_file_buf_cb on_retrieve_file_buf_cb, void *ctx)
{
    struct _CacheContext *context;
    struct _CacheEntry *entry;

    context = cache_context_create (size, ctx);
    context->cb.retrieve_cb = on_retrieve_file_buf_cb;
    entry = g_hash_table_lookup (cmng->h_entries, GUINT_TO_POINTER (ino));

    if (entry && range_contain (entry->avail_range, off, off + size)) {
        int fd;
        ssize_t res;
        char path[PATH_MAX];

        if (ino != entry->ino) {
            LOG_err (CMNG_LOG, "Requested inode doesn't match hashed key!");
            if (context->cb.retrieve_cb)
                context->cb.retrieve_cb (NULL, 0, FALSE, context->user_ctx);
            cache_context_destroy (context);
            return;
        }

        cache_mng_file_name (cmng, path, sizeof (path), ino);
        fd = open (path, O_RDONLY);

        context->buf = g_malloc (size);
        res = pread (fd, context->buf, size, off);
        close (fd);
        context->success = (res == (ssize_t) size);
        if (!context->success) {
            g_free (context->buf);
            context->buf = NULL;
        }

        // move entry to the front of q_lru
        g_queue_unlink (cmng->q_lru, entry->ll_lru);
        g_queue_push_head_link (cmng->q_lru, entry->ll_lru);
    } else {
        LOG_debug (CMNG_LOG, "Entry isn't found or doesn't contain requested range: %"INO_FMT, INO ino);
    }

    context->ev = event_new (application_get_evbase (cmng->app), -1,  0,
                    cache_read_cb, context);
    // fire this event at once
    event_active (context->ev, 0, 0);
    event_add (context->ev, NULL);
}

static void cache_write_cb (G_GNUC_UNUSED evutil_socket_t fd, G_GNUC_UNUSED short flags, void *ctx)
{
    struct _CacheContext *context = (struct _CacheContext *) ctx;

    if (context->cb.store_cb)
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
    guint64 old_length, new_length;
    guint64 range_size;

    range_size = (guint64)off + (guint64) size;

    // remove data until we have at least size bytes of max_size left
    while (cmng->max_size < cmng->size + size && g_queue_peek_tail (cmng->q_lru)) {
        entry = (struct _CacheEntry *) g_queue_peek_tail (cmng->q_lru);

        cache_mng_remove_file (cmng, entry->ino);
    }

    context = cache_context_create (size, ctx);
    context->cb.store_cb = on_store_file_buf_cb;

    cache_mng_file_name (cmng, path, sizeof (path), ino);
    fd = open (path, O_WRONLY|O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    res = pwrite(fd, buf, size, off);
    close (fd);

    entry = g_hash_table_lookup (cmng->h_entries, GUINT_TO_POINTER (ino));

    if (!entry) {
        entry = cache_entry_create (ino);
        g_queue_push_head (cmng->q_lru, entry);
        entry->ll_lru = g_queue_peek_head_link (cmng->q_lru);
        g_hash_table_insert (cmng->h_entries, GUINT_TO_POINTER (ino), entry);
    }

    old_length = range_length (entry->avail_range);
    range_add (entry->avail_range, off, range_size);
    new_length = range_length (entry->avail_range);
    cmng->size += new_length - old_length;
    
    // update modification time
    entry->modification_time = time (NULL);
   
    context->success = (res == (ssize_t) size);

    context->ev = event_new (application_get_evbase (cmng->app), -1,  0,
                    cache_write_cb, context);
    // fire this event at once
    event_active (context->ev, 0, 0);
    event_add (context->ev, NULL);
}

// removes file from local storage
void cache_mng_remove_file (CacheMng *cmng, fuse_ino_t ino)
{
    struct _CacheEntry *entry;
    char path[PATH_MAX];

    entry = g_hash_table_lookup (cmng->h_entries, GUINT_TO_POINTER (ino));
    if (entry) {
        cmng->size -= range_length (entry->avail_range);
        g_queue_delete_link (cmng->q_lru, entry->ll_lru);
        g_hash_table_remove (cmng->h_entries, GUINT_TO_POINTER (ino));
        cache_mng_file_name (cmng, path, sizeof (path), ino);
        unlink (path);
        LOG_debug (CMNG_LOG, "Entry is removed: %"INO_FMT, INO ino);
    } else {
        LOG_debug (CMNG_LOG, "Entry not found: %"INO_FMT, INO ino);
    }
}

guint64 cache_mng_size (CacheMng *cmng)
{
    return cmng->size;
}

guint64 cache_mng_get_file_length (CacheMng *cmng, fuse_ino_t ino)
{
    struct _CacheEntry *entry;

    entry = g_hash_table_lookup (cmng->h_entries, GUINT_TO_POINTER (ino));
    if (!entry)
        return 0;

    return range_length (entry->avail_range);
}

// we can only get md5 of an object containing 1 range
// XXX: move code to separate thread
gboolean cache_mng_get_md5 (CacheMng *cmng, fuse_ino_t ino, gchar **md5str)
{
    struct _CacheEntry *entry;
    unsigned char digest[MD5_DIGEST_LENGTH];
    MD5_CTX md5ctx;
    ssize_t bytes;
    unsigned char data[1024];
    char path[PATH_MAX];
    size_t i;
    gchar *out;
    FILE *in;

    entry = g_hash_table_lookup (cmng->h_entries, GUINT_TO_POINTER (ino));
    if (!entry)
        return FALSE;
    
    if (range_count (entry->avail_range) != 1) {
        LOG_debug (CMNG_LOG, "Entry contains more than 1 range, can't take MD5 sum of such obeject !");
        return FALSE;
    }

    cache_mng_file_name (cmng, path, sizeof (path), ino);
    in = fopen (path, "rb");
    if (in == NULL) {
        LOG_debug (CMNG_LOG, "Cant open file for reading: %s", path);
        return FALSE;
    }

    MD5_Init (&md5ctx);
    while ((bytes = fread (data, 1, 1024, in)) != 0)
        MD5_Update (&md5ctx, data, bytes);
    MD5_Final (digest, &md5ctx);
    fclose (in);

    out = g_malloc (33);
    for (i = 0; i < 16; ++i)
        sprintf (&out[i*2], "%02x", (unsigned int)digest[i]);

    *md5str = out;

    return TRUE;
}
