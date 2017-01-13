/*
 * Copyright (C) 2012-2014 Paul Ionkin <paul.ionkin@gmail.com>
 * Copyright (C) 2012-2014 Skoobe GmbH. All rights reserved.
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

/*{{{ structs / func defs */

struct _CacheMng {
    Application *app;
    GHashTable *h_entries;
    GQueue *q_lru;
    guint64 size;
    guint64 max_size;
    gchar *cache_dir;
    time_t check_time; // last check time of stored objects

    // stats
    guint64 cache_hits;
    guint64 cache_miss;
};

struct _CacheEntry {
    fuse_ino_t ino;
    Range *avail_range;
    time_t modification_time;
    GList *ll_lru;
    gchar *version_id;
    gchar *etag;
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

#define CMNG_LOG "cmng"

static void cache_entry_destroy (gpointer data);
static void cache_mng_rm_cache_dir (CacheMng *cmng);
/*}}}*/

/*{{{ create / destroy */
CacheMng *cache_mng_create (Application *app)
{
    CacheMng *cmng;
    gchar *rnd_str;

    cmng = g_new0 (CacheMng, 1);
    cmng->app = app;
    cmng->h_entries = g_hash_table_new_full (g_direct_hash, g_direct_equal, NULL, cache_entry_destroy);
    cmng->q_lru = g_queue_new ();
    cmng->size = 0;
    cmng->check_time = time (NULL);
    // If "filesystem.cache_dir_max_megabyte_size" is set, use it, else use "filesystem.cache_dir_max_size"
    if (conf_node_exists (application_get_conf (cmng->app), "filesystem.cache_dir_max_megabyte_size")) {
        cmng->max_size = conf_get_uint (application_get_conf (cmng->app), "filesystem.cache_dir_max_megabyte_size");
        cmng->max_size *= 1024 * 1024;      // Convert from megabytes to bytes
    } else {
        cmng->max_size = conf_get_uint (application_get_conf (cmng->app), "filesystem.cache_dir_max_size");
    }
    LOG_debug (CMNG_LOG, "Maximum cache size (bytes): %"PRId64, cmng->max_size);
    // generate random folder name for storing cache
    rnd_str = get_random_string (20, TRUE);
    cmng->cache_dir = g_strdup_printf ("%s/%s",
        conf_get_string (application_get_conf (cmng->app), "filesystem.cache_dir"), rnd_str);
    g_free (rnd_str);
    cmng->cache_hits = 0;
    cmng->cache_miss = 0;

    cache_mng_rm_cache_dir (cmng);
    if (g_mkdir_with_parents (cmng->cache_dir, 0700) != 0) {
        LOG_err (CMNG_LOG, "Failed to create directory: %s", cmng->cache_dir);
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
    entry->version_id = NULL; // version not set
    entry->etag = NULL;

    return entry;
}

static void cache_entry_destroy (gpointer data)
{
    struct _CacheEntry * entry = (struct _CacheEntry*) data;

    range_destroy(entry->avail_range);
    if (entry->version_id)
        g_free (entry->version_id);
    if (entry->etag)
        g_free (entry->etag);
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

static void cache_context_destroy (struct _CacheContext* context)
{
    if (context->ev)
        event_free (context->ev);
    if (context->buf)
        g_free (context->buf);
    g_free (context);
}
/*}}}*/

/*{{{ utils */
static int cache_mng_file_name (CacheMng *cmng, char *buf, int buflen, fuse_ino_t ino)
{
    return snprintf (buf, buflen, "%s/cache_mng_%"INO_FMT"", cmng->cache_dir, INO ino);
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

static void cache_mng_rm_cache_dir (CacheMng *cmng)
{
    if (cmng->cache_dir)
        utils_del_tree (cmng->cache_dir, 1);
    else {
        LOG_err (CMNG_LOG, "Cache directory not found: %s", cmng->cache_dir);
    }
}

// return version ID of cached file
// return NULL if version ID is not set
const gchar *cache_mng_get_version_id (CacheMng *cmng, fuse_ino_t ino)
{
    struct _CacheEntry *entry;

    entry = g_hash_table_lookup (cmng->h_entries, GUINT_TO_POINTER (ino));
    if (!entry)
        return NULL;

    return entry->version_id;
}

void cache_mng_update_version_id (CacheMng *cmng, fuse_ino_t ino, const gchar *version_id)
{
    struct _CacheEntry *entry;

    entry = g_hash_table_lookup (cmng->h_entries, GUINT_TO_POINTER (ino));
    if (!entry)
        return;

    if (entry->version_id) {
        if (strcmp (entry->version_id, version_id)) {
            g_free (entry->version_id);
            entry->version_id = g_strdup (version_id);
        }
    } else
        entry->version_id = g_strdup (version_id);
}

// What was Amazon's AWS ETag for this inode, when we cached it?
const gchar *cache_mng_get_etag (CacheMng *cmng, fuse_ino_t ino)
{
    struct _CacheEntry *entry;

    entry = g_hash_table_lookup (cmng->h_entries, GUINT_TO_POINTER (ino));
    if (!entry)
        return NULL;

    return entry->etag;
}

gboolean cache_mng_update_etag (CacheMng *cmng, fuse_ino_t ino, const gchar *etag)
{
    struct _CacheEntry *entry;

    entry = g_hash_table_lookup (cmng->h_entries, GUINT_TO_POINTER (ino));
    if (!entry)
        return FALSE;

    if (entry->etag) {
        if (strcmp (entry->etag, etag)) {
            g_free (entry->etag);
            entry->etag = g_strdup (etag);
        }
    } else
        entry->etag = g_strdup (etag);

    return TRUE;
}
/*}}}*/

/*{{{ retrieve_file_buf */
static void cache_read_cb (G_GNUC_UNUSED evutil_socket_t fd, G_GNUC_UNUSED short flags, void *ctx)
{
    struct _CacheContext *context = (struct _CacheContext *) ctx;

    if (context->cb.retrieve_cb)
        context->cb.retrieve_cb (context->buf, context->size, context->success, context->user_ctx);
    cache_context_destroy (context);
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
            LOG_err (CMNG_LOG, INO_H"Requested inode doesn't match hashed key!", INO_T (ino));
            if (context->cb.retrieve_cb)
                context->cb.retrieve_cb (NULL, 0, FALSE, context->user_ctx);
            cache_context_destroy (context);
            cmng->cache_miss++;
            return;
        }

        cache_mng_file_name (cmng, path, sizeof (path), ino);
        fd = open (path, O_RDONLY);
        if (fd < 0) {
            LOG_err (CMNG_LOG, INO_H"Failed to open file for reading! Path: %s", INO_T (ino), path);
            if (context->cb.retrieve_cb)
                context->cb.retrieve_cb (NULL, 0, FALSE, context->user_ctx);
            cache_context_destroy (context);
            cmng->cache_miss++;
            return;
        }

        context->buf = g_malloc (size);
        res = pread (fd, context->buf, size, off);
        close (fd);
        context->success = (res == (ssize_t) size);

        LOG_debug (CMNG_LOG, INO_H"Read [%"OFF_FMT":%zu] bytes, result: %s",
            INO_T (ino), off, size, context->success ? "OK" : "Failed");

        if (!context->success) {
            g_free (context->buf);
            context->buf = NULL;

            cmng->cache_miss++;
        } else
            cmng->cache_hits++;

        // move entry to the front of q_lru
        g_queue_unlink (cmng->q_lru, entry->ll_lru);
        g_queue_push_head_link (cmng->q_lru, entry->ll_lru);
    } else {
        LOG_debug (CMNG_LOG, INO_H"Entry isn't found or doesn't contain requested range: [%"OFF_FMT": %"OFF_FMT"]",
            INO_T (ino), off, off + size);

        cmng->cache_miss++;
    }

    context->ev = event_new (application_get_evbase (cmng->app), -1,  0,
                    cache_read_cb, context);
    // fire this event at once
    event_active (context->ev, 0, 0);
    event_add (context->ev, NULL);
}
/*}}}*/

/*{{{ store_file_buf */
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
    time_t now;

    range_size = (guint64)(off + size);

    // limit the number of cache checks
    now = time (NULL);
    if (cmng->check_time < now && now - cmng->check_time >= 10) {
        // remove data until we have at least size bytes of max_size left
        while (cmng->max_size < cmng->size + size && g_queue_peek_tail (cmng->q_lru)) {
            entry = (struct _CacheEntry *) g_queue_peek_tail (cmng->q_lru);

            cache_mng_remove_file (cmng, entry->ino);
        }
        cmng->check_time = now;
    }

    context = cache_context_create (size, ctx);
    context->cb.store_cb = on_store_file_buf_cb;

    cache_mng_file_name (cmng, path, sizeof (path), ino);
    fd = open (path, O_WRONLY|O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd < 0) {
        LOG_err (CMNG_LOG, INO_H"Failed to create / open file for writing! Path: %s", INO_T (ino), path);
        if (context->cb.store_cb)
            context->cb.store_cb (FALSE, context->user_ctx);
        cache_context_destroy (context);
        return;
    }
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
    if (new_length >= old_length)
        cmng->size += new_length - old_length;
    else {
        LOG_err (CMNG_LOG, INO_H"New length is less than the old length !: %"G_GUINT64_FORMAT" <= %"G_GUINT64_FORMAT,
            INO_T (ino), new_length, old_length);
    }

    // update modification time
    entry->modification_time = time (NULL);

    context->success = (res == (ssize_t) size);

    LOG_debug (CMNG_LOG, INO_H"Written [%"OFF_FMT":%zu] bytes, result: %s",
        INO_T (ino), off, size, context->success ? "OK" : "Failed");

    context->ev = event_new (application_get_evbase (cmng->app), -1,  0,
                    cache_write_cb, context);
    // fire this event at once
    event_active (context->ev, 0, 0);
    event_add (context->ev, NULL);
}
/*}}}*/

/*{{{ remove_file*/
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
        LOG_debug (CMNG_LOG, INO_H"Entry is removed", INO_T (ino));
    } else {
        LOG_debug (CMNG_LOG, INO_H"Entry not found", INO_T (ino));
    }
}
/*}}}*/

/*{{{ get_stats*/
void cache_mng_get_stats (CacheMng *cmng, guint32 *entries_num, guint64 *total_size, guint64 *cache_hits, guint64 *cache_miss)
{
    GHashTableIter iter;
    struct _CacheEntry *entry;
    gpointer value;

    *entries_num = g_hash_table_size (cmng->h_entries);
    *cache_hits = cmng->cache_hits;
    *cache_miss = cmng->cache_miss;
    *total_size = 0;

    g_hash_table_iter_init (&iter, cmng->h_entries);
    while (g_hash_table_iter_next (&iter, NULL, &value)) {
        entry = (struct _CacheEntry *) value;
        *total_size = *total_size + range_length (entry->avail_range);
    }

}/*}}}*/
