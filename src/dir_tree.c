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
#include "dir_tree.h"
#include "rfuse.h"
#include "http_connection.h"
#include "client_pool.h"
#include "file_io_ops.h"
#include "cache_mng.h"
#include "utils.h"

/*{{{ struct / defines*/

struct _DirEntry {
    fuse_ino_t ino;
    fuse_ino_t parent_ino;
    gchar *basename;
    gchar *fullpath;
    guint64 age; // if age >= parent's age, then show entry in directory listing
    gboolean removed;
    
    // type of directory entry
    DirEntryType type;

    gboolean is_modified; // do not show it

    guint64 size;
    mode_t mode;
    time_t ctime;

    // for type == DET_dir
    char *dir_cache; // FUSE directory cache
    size_t dir_cache_size; // directory cache size
    time_t dir_cache_created;

    GHashTable *h_dir_tree; // name -> data

    gboolean is_updating; // TRUE if getting attributes
    time_t updated_time; // time when entry was updated
    time_t access_time; // time when entry was accessed

    gchar *etag; // S3 md5
    gchar *version_id;
    gchar *content_type;
    time_t xattr_time; // time when XAttrs were updated
};

struct _DirTree {
    DirEntry *root;
    GHashTable *h_inodes; // inode -> DirEntry
    Application *app;

    fuse_ino_t max_ino;
    guint64 current_age;

    gint64 current_write_ops; // the number of current write operations
};

#define DIR_TREE_LOG "dir_tree"
#define DIR_DEFAULT_MODE S_IFDIR | 0755
#define FILE_DEFAULT_MODE S_IFREG | 0644
/*}}}*/

/*{{{ func declarations */
static DirEntry *dir_tree_add_entry (DirTree *dtree, const gchar *basename, mode_t mode, 
    DirEntryType type, fuse_ino_t parent_ino, off_t size, time_t ctime);
static void dir_tree_entry_modified (DirTree *dtree, DirEntry *en);
static void dir_entry_destroy (gpointer data);
/*}}}*/

/*{{{ create / destroy */

DirTree *dir_tree_create (Application *app)
{
    DirTree *dtree;

    dtree = g_new0 (DirTree, 1);
    dtree->app = app;
    // children entries are destroyed by parent directory entries
    dtree->h_inodes = g_hash_table_new (g_direct_hash, g_direct_equal);
    dtree->max_ino = FUSE_ROOT_ID;
    dtree->current_age = 0;
    dtree->current_write_ops = 0;

    dtree->root = dir_tree_add_entry (dtree, "/", DIR_DEFAULT_MODE, DET_dir, 0, 0, time (NULL));

    LOG_debug (DIR_TREE_LOG, "DirTree created");

    return dtree;
}

void dir_tree_destroy (DirTree *dtree)
{
    g_hash_table_destroy (dtree->h_inodes);
    dir_entry_destroy (dtree->root);
    g_free (dtree);
}
/*}}}*/

/*{{{ dir_entry operations */
static void dir_entry_destroy (gpointer data)
{
    DirEntry *en = (DirEntry *) data;

    if (!en)
        return;

    // recursively delete entries
    if (en->h_dir_tree)
        g_hash_table_destroy (en->h_dir_tree);
    if (en->dir_cache)
        g_free (en->dir_cache);
    if (en->etag)
        g_free (en->etag);
    if (en->version_id)
        g_free (en->version_id);
    if (en->content_type)
        g_free (en->content_type);

    g_free (en->basename);
    g_free (en->fullpath);
    g_free (en);
}

// create and add a new entry (file or dir) to DirTree
static DirEntry *dir_tree_add_entry (DirTree *dtree, const gchar *basename, mode_t mode, 
    DirEntryType type, fuse_ino_t parent_ino, off_t size, time_t ctime)
{
    DirEntry *en;
    DirEntry *parent_en = NULL;
    gchar *fullpath = NULL;
    char tmbuf[64];
    struct tm *nowtm;

    // get the parent, for inodes > 0
    if (parent_ino) {
        parent_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
        if (!parent_en) {
            LOG_err (DIR_TREE_LOG, "Parent not found for ino: %"INO_FMT" !", INO parent_ino);
            return NULL;
        }
    }

    if (parent_en) {
        // check if parent already contains file with the same name.
        en = g_hash_table_lookup (parent_en->h_dir_tree, basename);
        if (en && en->type != type) {
            LOG_debug (DIR_TREE_LOG, "Parent already contains file %s!", basename);
            return NULL;
        }
    }

    // get fullname
    if (parent_ino) {
        // update directory buffer
        dir_tree_entry_modified (dtree, parent_en);

        if (parent_ino == FUSE_ROOT_ID)
            fullpath = g_strdup_printf ("%s", basename);
        else
            fullpath = g_strdup_printf ("%s/%s", parent_en->fullpath, basename);
    } else {
        fullpath = g_strdup ("");
    }

    en = g_new0 (DirEntry, 1);
    en->is_updating = FALSE;
    en->fullpath = fullpath;
    en->ino = dtree->max_ino++;
    en->age = dtree->current_age;
    en->basename = g_strdup (basename);
    en->mode = mode;
    en->size = size;
    en->parent_ino = parent_ino;
    en->type = type;
    en->ctime = ctime;
    en->is_modified = FALSE;
    en->removed = FALSE;
    en->updated_time = 0;
    en->access_time = time (NULL);
    en->xattr_time = 0;

    // cache is empty
    en->dir_cache = NULL;
    en->dir_cache_size = 0;
    en->dir_cache_created = 0;

    nowtm = localtime (&en->ctime);
    strftime (tmbuf, sizeof (tmbuf), "%Y-%m-%d %H:%M:%S", nowtm);

    LOG_debug (DIR_TREE_LOG, INO_H"Creating new DirEntry: %s, fullpath: %s, mode: %d time: %s", 
        INO_T (en->ino), en->basename, en->fullpath, en->mode, tmbuf);
   
    if (type == DET_dir) {
        en->h_dir_tree = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, dir_entry_destroy);
    }
    
    // add to global inode hash
    g_hash_table_insert (dtree->h_inodes, GUINT_TO_POINTER (en->ino), en);

    // add to the parent's hash
    if (parent_ino)
        g_hash_table_insert (parent_en->h_dir_tree, g_strdup (en->basename), en);

    // inform parent that the directory cache has changed
    if (parent_ino)
        dir_tree_entry_modified (dtree, parent_en);
    
    return en;
}

static gboolean dir_tree_is_cache_expired (DirTree *dtree, DirEntry *en)
{
    time_t t;

    // cache is not filled
    if (!en->dir_cache_size || !en->dir_cache_created)
        return TRUE;

    t = time (NULL);

    // make sure "now" is greater than cache time
    if (t < en->dir_cache_created)
        return FALSE;
    
    // is it expired
    if (t - en->dir_cache_created > (time_t)conf_get_uint (application_get_conf (dtree->app), "filesystem.dir_cache_max_time"))
        return TRUE;

    return FALSE;
}

// increase the age of directory
void dir_tree_start_update (DirTree *dtree, G_GNUC_UNUSED const gchar *dir_path)
{
    //XXX: per directory ?
    dtree->current_age++;
}

// remove DirEntry, which age is lower than the current
static gboolean dir_tree_stop_update_on_remove_child_cb (gpointer key, gpointer value, gpointer ctx)
{
    DirTree *dtree = (DirTree *)ctx;
    DirEntry *en = (DirEntry *) value;
    const gchar *name = (const gchar *) key;
    time_t now = time (NULL);

    // if entry is "old", but someone still tries to access it - leave it untouched
    // XXX: implement smarter algorithm here, "time to remove" should be based on the number of hits
    // process files only 
    if (en->age < dtree->current_age && 
        !en->is_modified && 
        now > en->access_time && 
        (guint32)(now - en->access_time) >= conf_get_uint (application_get_conf (dtree->app), "filesystem.dir_cache_max_time") &&
        en->type != DET_dir) {

        // first remove item from the inode hash table !
        g_hash_table_remove (dtree->h_inodes, GUINT_TO_POINTER (en->ino));
    
        // now remove from parent's hash table, it will call destroy () fucntion
        if (en->type == DET_dir) {
            // XXX:
            LOG_debug (DIR_TREE_LOG, "Removing dir: %s", en->fullpath);
            return TRUE;
        } else {
            LOG_debug (DIR_TREE_LOG, "Removing file %s", name);
            //XXX:
            return TRUE;
        }
    }

    return FALSE;
}

// remove all entries which age is less than current
void dir_tree_stop_update (DirTree *dtree, fuse_ino_t parent_ino)
{
    DirEntry *parent_en;
    guint res;

    parent_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    if (!parent_en || parent_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "DirEntry is not a directory ! ino: %"INO_FMT, INO parent_ino);
        return;
    }
    LOG_debug (DIR_TREE_LOG, "Removing old DirEntries for: %s ..", parent_en->fullpath);

    if (parent_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "Parent is not a directory !");
        return;
    }
    
    res = g_hash_table_foreach_remove (parent_en->h_dir_tree, dir_tree_stop_update_on_remove_child_cb, dtree);
    if (res)
        LOG_debug (DIR_TREE_LOG, "Removed: %u entries !", res);
}

DirEntry *dir_tree_update_entry (DirTree *dtree, G_GNUC_UNUSED const gchar *path, DirEntryType type, 
    fuse_ino_t parent_ino, const gchar *entry_name, long long size, time_t last_modified)
{
    DirEntry *parent_en;
    DirEntry *en;

    
    // get parent
    parent_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    if (!parent_en || parent_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "DirEntry is not a directory ! ino: %"INO_FMT, INO parent_ino);
        return NULL;
    }

    // get child
    en = g_hash_table_lookup (parent_en->h_dir_tree, entry_name);
    if (en) {
        en->age = dtree->current_age;
        en->size = size;
        // we got this entry from the server, mark as existing file
        en->removed = FALSE;
    } else {
        mode_t mode;

        if (type == DET_file)
            mode = FILE_DEFAULT_MODE;
        else
            mode = DIR_DEFAULT_MODE;
            
        en = dir_tree_add_entry (dtree, entry_name, mode,
            type, parent_ino, size, last_modified);
    }

    LOG_debug (DIR_TREE_LOG, INO_H"Updating %s %ld", INO_T (en->ino), entry_name, size);

    return en;
}

// let it know that directory cache have to be updated
static void dir_tree_entry_modified (DirTree *dtree, DirEntry *en)
{
    if (en->type == DET_dir) {
        if (en->dir_cache)
            g_free (en->dir_cache);
        en->dir_cache = NULL;
        en->dir_cache_size = 0;
        en->dir_cache_created = 0;

        LOG_debug (DIR_TREE_LOG, INO_H"Invalidating cache for directory: %s", INO_T (en->ino), en->basename);
    } else {
        DirEntry *parent_en;
        
        parent_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (en->parent_ino));
        if (!parent_en || parent_en->type != DET_dir) {
            LOG_err (DIR_TREE_LOG, INO_H"Parent not found! %", INO_T (en->ino));
            return;
        }

        dir_tree_entry_modified (dtree, parent_en);
    }
}
/*}}}*/

/*{{{ dir_tree_fill_dir_buf */

typedef struct {
    DirTree *dtree;
    fuse_ino_t ino;
    guint64 size;
    off_t off;
    dir_tree_readdir_cb readdir_cb;
    fuse_req_t req;
    gpointer ctx;
} DirTreeFillDirData;

// callback: directory structure
void dir_tree_fill_on_dir_buf_cb (gpointer callback_data, gboolean success)
{
    DirTreeFillDirData *dir_fill_data = (DirTreeFillDirData *) callback_data;
    DirEntry *en;

    en = g_hash_table_lookup (dir_fill_data->dtree->h_inodes, GUINT_TO_POINTER (dir_fill_data->ino));
    if (!en) {
        LOG_err (DIR_TREE_LOG, INO_H"Entry not found!", INO_T (dir_fill_data->ino));
        dir_fill_data->readdir_cb (dir_fill_data->req, FALSE, dir_fill_data->size, dir_fill_data->off, NULL, 0, dir_fill_data->ctx);
        g_free (dir_fill_data);
        return;
    }
    
    LOG_debug (DIR_TREE_LOG, "[ino: %"INO_FMT" req: %p] Dir fill callback: %s", 
        INO_T (dir_fill_data->ino), dir_fill_data->req, success ? "SUCCESS" : "FAILED");
    

    if (!success) {
        dir_fill_data->readdir_cb (dir_fill_data->req, FALSE, dir_fill_data->size, dir_fill_data->off, NULL, 0, dir_fill_data->ctx);
    } else {
        struct dirbuf b; // directory buffer
        GHashTableIter iter;
        gpointer value;

        // construct directory buffer
        // add "." and ".."
        memset (&b, 0, sizeof(b));
        rfuse_add_dirbuf (dir_fill_data->req, &b, ".", dir_fill_data->ino, 0);
        rfuse_add_dirbuf (dir_fill_data->req, &b, "..", dir_fill_data->ino, 0);

        LOG_debug (DIR_TREE_LOG, INO_H"Entries in directory : %u", INO_T (dir_fill_data->ino), g_hash_table_size (en->h_dir_tree));
        
        // get all directory items
        g_hash_table_iter_init (&iter, en->h_dir_tree);
        while (g_hash_table_iter_next (&iter, NULL, &value)) {
            DirEntry *tmp_en = (DirEntry *) value;
            // add only updated entries
            if (tmp_en->age >= dir_fill_data->dtree->current_age) {
                rfuse_add_dirbuf (dir_fill_data->req, &b, tmp_en->basename, tmp_en->ino, tmp_en->size);
            } else {
                LOG_debug (DIR_TREE_LOG, INO_H"Entry %s is removed from directory listing!", 
                    INO_T (tmp_en->ino), tmp_en->basename);
            }
        }
        
        // done, save as cache
        if (en->dir_cache)
            g_free (en->dir_cache);

        en->dir_cache_size = b.size;
        en->dir_cache = g_malloc0 (b.size);

        memcpy (en->dir_cache, b.p, b.size);
        // send buffer to fuse
        dir_fill_data->readdir_cb (dir_fill_data->req, TRUE, dir_fill_data->size, dir_fill_data->off, 
            b.p, b.size, dir_fill_data->ctx);

        //free buffer
        g_free (b.p);
        
        en->dir_cache_created = time (NULL);
        LOG_debug (DIR_TREE_LOG, INO_H"Dir cache updated: %d", INO_T (dir_fill_data->ino), en->dir_cache_created);
    }

    g_free (dir_fill_data);
}

static void dir_tree_fill_dir_on_http_ready (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    DirTreeFillDirData *dir_fill_data = (DirTreeFillDirData *) ctx;
    DirEntry *en;

    en = g_hash_table_lookup (dir_fill_data->dtree->h_inodes, GUINT_TO_POINTER (dir_fill_data->ino));
    if (!en) {
        LOG_err (DIR_TREE_LOG, INO_H"Entry not found!", INO_T (dir_fill_data->ino));
        dir_fill_data->readdir_cb (dir_fill_data->req, FALSE, dir_fill_data->size, dir_fill_data->off, NULL, 0, dir_fill_data->ctx);
        g_free (dir_fill_data);
        return;
    }

    //send http request
    http_connection_get_directory_listing (con, 
        en->fullpath, dir_fill_data->ino,
        dir_tree_fill_on_dir_buf_cb, dir_fill_data
    );
}

// return directory buffer from the cache
// or regenerate directory cache
void dir_tree_fill_dir_buf (DirTree *dtree, 
        fuse_ino_t ino, size_t size, off_t off,
        dir_tree_readdir_cb readdir_cb, fuse_req_t req,
        gpointer ctx)
{
    DirEntry *en;
    DirTreeFillDirData *dir_fill_data;
    
    LOG_debug (DIR_TREE_LOG, INO_H"Requesting directory buffer: size: %zd, off: %"OFF_FMT, INO_T (ino), size, off);
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if directory does not exist
    // or it's not a directory type ?
    if (!en || en->type != DET_dir) {
        LOG_msg (DIR_TREE_LOG, INO_H"Directory not found !", INO_T (ino));
        readdir_cb (req, FALSE, size, off, NULL, 0, ctx);
        return;
    }
    
    // already have directory buffer in the cache
    if (!dir_tree_is_cache_expired (dtree, en)) {
        LOG_debug (DIR_TREE_LOG, INO_H"Sending directory buffer from cache !", INO_T (ino));
        readdir_cb (req, TRUE, size, off, en->dir_cache, en->dir_cache_size, ctx);
        return;
    }

    // make sure that subsequent requests return the same directory structure
    if (off > 0) {
        // update timer, if subsequent requests has the same "req" as the one which updated dir cache
        //XXX: dir_fill_data->en->dir_cache_created = time (NULL);
        LOG_debug (DIR_TREE_LOG, INO_H"Sending directory buffer from cache !", INO_T (ino));
        readdir_cb (req, TRUE, size, off, en->dir_cache, en->dir_cache_size, ctx);
        return;
    }

    // reset dir cache
    if (en->dir_cache)
        g_free (en->dir_cache);
    en->dir_cache = NULL;
    en->dir_cache_size = 0;
    en->dir_cache_created = 0;

    dir_fill_data = g_new0 (DirTreeFillDirData, 1);
    dir_fill_data->dtree = dtree;
    dir_fill_data->ino = ino;
    dir_fill_data->size = size;
    dir_fill_data->off = off;
    dir_fill_data->readdir_cb = readdir_cb;
    dir_fill_data->req = req;
    dir_fill_data->ctx = ctx;

    if (!client_pool_get_client (application_get_ops_client_pool (dtree->app), dir_tree_fill_dir_on_http_ready, dir_fill_data)) {
        LOG_err (DIR_TREE_LOG, "Failed to get http client !");
        readdir_cb (req, FALSE, size, off, NULL, 0, ctx);
        g_free (dir_fill_data);
    }
}
/*}}}*/

/*{{{ dir_tree_lookup */

typedef struct {
    DirTree *dtree;
    dir_tree_lookup_cb lookup_cb;
    fuse_req_t req;
    fuse_ino_t ino;

    gboolean not_found;
    char *name;
    fuse_ino_t parent_ino;
} LookupOpData;

static void dir_tree_on_lookup_cb (HttpConnection *con, void *ctx, gboolean success,
    G_GNUC_UNUSED const gchar *buf, G_GNUC_UNUSED size_t buf_len, 
    struct evkeyvalq *headers)
{
    LookupOpData *op_data = (LookupOpData *) ctx;
    const gchar *size_header;
    const gchar *content_type;
    DirEntry  *en;
    
    LOG_debug (DIR_TREE_LOG, INO_H"Got attributes", INO_T (op_data->ino));

    // release HttpConnection
    http_connection_release (con);

    en = g_hash_table_lookup (op_data->dtree->h_inodes, GUINT_TO_POINTER (op_data->ino));
    // entry not found
    if (!en) {
        LOG_debug (DIR_TREE_LOG, INO_H"Entry not found !", INO_T (op_data->ino));
        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data);
        return;
    }

    if (!success) {
        LOG_debug (DIR_TREE_LOG, INO_H"Failed to get entry attributes !", INO_T (op_data->ino));
        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data);
        en->is_updating = FALSE;
        return;
    }

    // get Content-Length header
    size_header = http_find_header (headers, "Content-Length");
    if (size_header) {
        gint64 size;
        size = strtoll ((char *)size_header, NULL, 10);
        if (size < 0) {
            LOG_err (DIR_TREE_LOG, INO_H"Header contains incorrect file size!", INO_T (op_data->ino));
            size = 0;
        }
        en->size = size;
    }

    dir_tree_entry_update_xattrs (en, headers);
    
    // check if this is a directory
    content_type = http_find_header (headers, "Content-Type");
    if (content_type && !strncmp ((const char *)content_type, "application/x-directory", strlen ("application/x-directory"))) {
        en->type = DET_dir;
        en->mode = DIR_DEFAULT_MODE;
        
        if (!en->h_dir_tree)
            en->h_dir_tree = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, dir_entry_destroy);

        if (en->dir_cache)
            g_free (en->dir_cache);
        en->dir_cache = NULL;
        en->dir_cache_size = 0;
        en->dir_cache_created = 0;
        
        LOG_debug (DIR_TREE_LOG, INO_H"Converting to directory: %s", INO_T (en->ino), en->fullpath);
    }

    op_data->lookup_cb (op_data->req, TRUE, en->ino, en->mode, en->size, en->ctime);
    en->is_updating = FALSE;
    en->updated_time = time (NULL);
    g_free (op_data);
}

//send http HEAD request
static void dir_tree_on_lookup_con_cb (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    LookupOpData *op_data = (LookupOpData *) ctx;
    gchar *req_path = NULL;
    gboolean res;
    DirEntry  *en;

    en = g_hash_table_lookup (op_data->dtree->h_inodes, GUINT_TO_POINTER (op_data->ino));
    // entry not found
    if (!en) {
        LOG_debug (DIR_TREE_LOG, INO_H"Entry not found !", INO_T (op_data->ino));
        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data);
        return;
    }

    http_connection_acquire (con);

    req_path = g_strdup_printf ("/%s", en->fullpath);

    res = http_connection_make_request (con, 
        req_path, "HEAD", NULL,
        dir_tree_on_lookup_cb,
        op_data
    );

    g_free (req_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, INO_H"Failed to create http request !", INO_T (op_data->ino));
        http_connection_release (con);
        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        en->is_updating = FALSE;
        g_free (op_data);
        return;
    }
}

static void dir_tree_on_lookup_not_found_cb (HttpConnection *con, void *ctx, gboolean success,
    G_GNUC_UNUSED const gchar *buf, G_GNUC_UNUSED size_t buf_len, 
    struct evkeyvalq *headers)
{
    LookupOpData *op_data = (LookupOpData *) ctx;
    const char *size_header;
    const char *last_modified_header;
    DirEntry *en;
    time_t last_modified = time (NULL);
    DirEntry *parent_en;
    gint64 size = 0;
    
    LOG_debug (DIR_TREE_LOG, INO_H"Got attributes !", INO_T (op_data->ino));

    // release HttpConnection
    http_connection_release (con);

    parent_en = g_hash_table_lookup (op_data->dtree->h_inodes, GUINT_TO_POINTER (op_data->parent_ino));
    if (!parent_en) {
        LOG_debug (DIR_TREE_LOG, INO_H"Parent not found for ino: %"INO_FMT" !", INO_T (op_data->ino), INO op_data->parent_ino);

        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data->name);
        g_free (op_data);
        return;
    }

    // file not found
    if (!success) {
        LOG_debug (DIR_TREE_LOG, INO_H"FileEntry not found %s", INO_T (op_data->ino), op_data->name);

        // create a temporary entry to hold this object
        // this is required to avoid further HEAD requests
        en = dir_tree_add_entry (op_data->dtree, op_data->name, FILE_DEFAULT_MODE, DET_file, op_data->parent_ino, 0, time (NULL));
        if (!en) {
            LOG_err (DIR_TREE_LOG, INO_H"Failed to create file: %s !", INO_T (op_data->ino), op_data->name);
        } else {
            // set as removed
            en->removed = TRUE;
        }

        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data->name);
        g_free (op_data);
        return;
    }

    // get Content-Length header
    size_header = http_find_header (headers, "Content-Length");
    if (size_header) {
        size = strtoll ((char *)size_header, NULL, 10);
        if (size < 0) {
            LOG_err (DIR_TREE_LOG, INO_H"Header contains incorrect file size!", INO_T (op_data->ino));
            size = 0;
        }
    }

    last_modified_header = http_find_header (headers, "Last-Modified");
    if (last_modified_header) {
        struct tm tmp = {0};
        // Sun, 1 Jan 2006 12:00:00
        if (strptime (last_modified_header, "%a, %d %b %Y %H:%M:%S", &tmp))
            last_modified = mktime (&tmp);
    }
    
    en = dir_tree_update_entry (op_data->dtree, parent_en->fullpath, DET_file, 
        op_data->parent_ino, op_data->name, size, last_modified);

    if (!en) {
        LOG_err (DIR_TREE_LOG, INO_H"Failed to create FileEntry parent ino: %"INO_FMT" !",
            INO_T (op_data->ino), INO op_data->parent_ino);

        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data->name);
        g_free (op_data);
        return;
    }
    
    dir_tree_entry_update_xattrs (en, headers);

    op_data->lookup_cb (op_data->req, TRUE, en->ino, en->mode, en->size, en->ctime);
    g_free (op_data->name);
    g_free (op_data);
}

//send http HEAD request when a file "not found"
static void dir_tree_on_lookup_not_found_con_cb (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    LookupOpData *op_data = (LookupOpData *) ctx;
    gchar *req_path = NULL;
    gboolean res;
    DirEntry *parent_en;
    gchar *fullpath;

    parent_en = g_hash_table_lookup (op_data->dtree->h_inodes, GUINT_TO_POINTER (op_data->parent_ino));
    if (!parent_en) {
        LOG_err (DIR_TREE_LOG, INO_H"Parent not found for ino: %"INO_FMT" !", INO_T (op_data->ino), INO op_data->parent_ino);

        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data->name);
        g_free (op_data);
        return;
    }

    http_connection_acquire (con);

    if (op_data->parent_ino == FUSE_ROOT_ID)
        fullpath = g_strdup_printf ("%s", op_data->name);
    else
        fullpath = g_strdup_printf ("%s/%s", parent_en->fullpath, op_data->name);

    req_path = g_strdup_printf ("/%s", fullpath);

    g_free (fullpath);

    res = http_connection_make_request (con, 
        req_path, "HEAD", NULL,
        dir_tree_on_lookup_not_found_cb,
        op_data
    );

    g_free (req_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create http request !");
        http_connection_release (con);
        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data->name);
        g_free (op_data);
        return;
    }
}

static void dir_tree_on_lookup_read (G_GNUC_UNUSED fuse_req_t req, gboolean success, 
    G_GNUC_UNUSED size_t max_size, G_GNUC_UNUSED off_t off,
    G_GNUC_UNUSED const char *buf, G_GNUC_UNUSED size_t buf_size, 
    gpointer ctx)
{
    LookupOpData *op_data = (LookupOpData *) ctx;

    if (!success) {
        LOG_err (DIR_TREE_LOG, "Failed to get directory listing !");
        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data->name);
        g_free (op_data);
        return;
    }

    // directory cache is filled, repeat search
    // XXX: add recursion protection !!
    dir_tree_lookup (op_data->dtree, op_data->parent_ino, op_data->name,
        op_data->lookup_cb, op_data->req);
    g_free (op_data->name);
    g_free (op_data);
}

// lookup entry and return attributes
void dir_tree_lookup (DirTree *dtree, fuse_ino_t parent_ino, const char *name,
    dir_tree_lookup_cb lookup_cb, fuse_req_t req)
{
    DirEntry *dir_en, *en;
    time_t t;
    
    LOG_debug (DIR_TREE_LOG, "Looking up for '%s' in directory ino: %"INO_FMT, name, INO parent_ino);

    dir_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    
    // entry not found or not a dir
    if (!dir_en || dir_en->type != DET_dir) {
        LOG_msg (DIR_TREE_LOG, "Directory not found ! ino: %"INO_FMT, INO parent_ino);
        lookup_cb (req, FALSE, 0, 0, 0, 0);
        return;
    }

    // directory cache is expired
    // XXX: add recursion protection !!
    if (dir_tree_is_cache_expired (dtree, dir_en)) {
        
        LookupOpData *op_data;
        
        op_data = g_new0 (LookupOpData, 1);
        op_data->dtree = dtree;
        op_data->lookup_cb = lookup_cb;
        op_data->req = req;
        op_data->not_found = TRUE;
        op_data->parent_ino = parent_ino;
        op_data->name = g_strdup (name);

        LOG_debug (DIR_TREE_LOG, INO_H"Getting directory listing ..", INO_T (dir_en->ino));
        dir_tree_fill_dir_buf (dtree, dir_en->ino, 1024*1024*1, 0, dir_tree_on_lookup_read, req, op_data);

        return;
    }    

    en = g_hash_table_lookup (dir_en->h_dir_tree, name);
    if (!en) {
        LookupOpData *op_data;

        //XXX: CacheMng !

        op_data = g_new0 (LookupOpData, 1);
        op_data->dtree = dtree;
        op_data->lookup_cb = lookup_cb;
        op_data->req = req;
        op_data->not_found = TRUE;
        op_data->parent_ino = parent_ino;
        op_data->name = g_strdup (name);

        LOG_debug (DIR_TREE_LOG, INO_H"Entry (%s) not found, sending request to the server.", INO_T (dir_en->ino), name);
        
        if (!client_pool_get_client (application_get_ops_client_pool (op_data->dtree->app),
            dir_tree_on_lookup_not_found_con_cb, op_data)) 
        {
            LOG_err (DIR_TREE_LOG, "Failed to get http client !");
            op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
            g_free (op_data->name);
            g_free (op_data);
        }
        return;
    }

    // update access time
    en->access_time = time (NULL);

    // file is removed
    if (en->removed) {
        LOG_debug (DIR_TREE_LOG, INO_H"Entry '%s' is removed !", INO_T (en->ino), name);
        lookup_cb (req, FALSE, 0, 0, 0, 0);
        return;
    }

    // get extra info for file
    /*
    if (!en->is_updating) {
        LookupOpData *op_data;

        //XXX: CacheMng !

        op_data = g_new0 (LookupOpData, 1);
        op_data->dtree = dtree;
        op_data->lookup_cb = lookup_cb;
        op_data->req = req;
        op_data->ino = en->ino;
        op_data->not_found = FALSE;

        en->is_updating = TRUE;

        LOG_debug (DIR_TREE_LOG, "Getting information, ino: %"INO_FMT, INO en->ino);

        if (!client_pool_get_client (application_get_ops_client_pool (dtree->app), dir_tree_lookup_on_con_cb, op_data)) {
            LOG_err (DIR_TREE_LOG, "Failed to get http client !");
            lookup_cb (req, FALSE, 0, 0, 0, 0);
            en->is_updating = FALSE;
            g_free (op_data);
        }

        return;
    }
    */

    if (en->is_modified && !en->is_updating && en->type == DET_dir) {

        LookupOpData *op_data;

        //XXX: CacheMng !

        op_data = g_new0 (LookupOpData, 1);
        op_data->dtree = dtree;
        op_data->lookup_cb = lookup_cb;
        op_data->req = req;
        op_data->ino = en->ino;
        op_data->not_found = FALSE;

        en->is_updating = TRUE;

        LOG_debug (DIR_TREE_LOG, INO_H"Entry '%s' is modified !", INO_T (en->ino), name);
        
        
        if (!client_pool_get_client (application_get_ops_client_pool (dtree->app), dir_tree_on_lookup_con_cb, op_data)) {
            LOG_err (DIR_TREE_LOG, "Failed to get http client !");
            lookup_cb (req, FALSE, 0, 0, 0, 0);
            en->is_updating = FALSE;
            g_free (op_data);
        }
        
        // lookup_cb (req, TRUE, en->ino, en->mode, en->size, en->ctime);
        return;
    }
    
    t = time (NULL);

    // compatibility with s3fs: send HEAD request to server if file size is 0 to check if it's a directory
    if (!en->is_updating && en->type == DET_file && en->size == 0 && t >= en->updated_time &&
        t - en->updated_time >= (time_t)conf_get_uint (application_get_conf (dtree->app), "filesystem.dir_cache_max_time") &&
        conf_get_boolean (application_get_conf (dtree->app), "s3.check_empty_files")) {

        LookupOpData *op_data;

        //XXX: CacheMng !
        LOG_debug (DIR_TREE_LOG, INO_H"FILE has 0 lenght: %s", INO_T (en->ino), en->fullpath);

        op_data = g_new0 (LookupOpData, 1);
        op_data->dtree = dtree;
        op_data->lookup_cb = lookup_cb;
        op_data->req = req;
        op_data->ino = en->ino;
        op_data->not_found = FALSE;

        en->is_updating = TRUE;

        if (!client_pool_get_client (application_get_ops_client_pool (dtree->app), dir_tree_on_lookup_con_cb, op_data)) {
            LOG_err (DIR_TREE_LOG, "Failed to get http client !");
            lookup_cb (req, FALSE, 0, 0, 0, 0);
            en->is_updating = FALSE;
            g_free (op_data);
        }

        return;
    }

    lookup_cb (req, TRUE, en->ino, en->mode, en->size, en->ctime);
}
/*}}}*/

/*{{{ dir_tree_getattr */

typedef struct {
    DirTree *dtree;
    dir_tree_getattr_cb getattr_cb;
    fuse_req_t req;
    fuse_ino_t ino;
} GetAttrOpData;
/*
static void dir_tree_getattr_on_attr_cb (HttpConnection *con, void *ctx, gboolean success,
    G_GNUC_UNUSED const gchar *buf, G_GNUC_UNUSED size_t buf_len, 
    G_GNUC_UNUSED struct evkeyvalq *headers)
{
    GetAttrOpData *op_data = (GetAttrOpData *) ctx;
    DirEntry  *en;

    // release HttpConnection
    http_connection_release (con);

    en = g_hash_table_lookup (op_data->dtree->h_inodes, GUINT_TO_POINTER (op_data->ino));
    
    // entry not found
    if (!en) {
        LOG_err (DIR_TREE_LOG, "Entry (%"INO_FMT") not found !", INO op_data->ino);
        op_data->getattr_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data);
        return;
    }

    if (!success) {
        LOG_err (DIR_TREE_LOG, "Failed to get entry (%"INO_FMT") attributes !", INO op_data->ino);
        op_data->getattr_cb (op_data->req, FALSE, 0, 0, 0, 0);
        en->is_updating = FALSE;
        g_free (op_data);
        return;
    }

    LOG_debug (DIR_TREE_LOG, "Got attributes for ino: %"INO_FMT" size: %zu", INO op_data->ino, en->size);
    
    en->is_updating = FALSE;
    op_data->getattr_cb (op_data->req, TRUE, en->ino, en->mode, en->size, en->ctime);
    g_free (op_data);
}

//send http HEAD request
static void dir_tree_getattr_on_con_cb (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    GetAttrOpData *op_data = (GetAttrOpData *) ctx;
    gchar *req_path = NULL;
    gboolean res;
    DirEntry  *en;

    en = g_hash_table_lookup (op_data->dtree->h_inodes, GUINT_TO_POINTER (op_data->ino));
    
    // entry not found
    if (!en) {
        LOG_err (DIR_TREE_LOG, "Entry (%"INO_FMT") not found !", INO op_data->ino);
        op_data->getattr_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data);
        return;
    }

    http_connection_acquire (con);

    req_path = g_strdup_printf ("/%s", en->fullpath);

    res = http_connection_make_request (con, 
        req_path, "HEAD", NULL,
        dir_tree_getattr_on_attr_cb,
        op_data
    );

    g_free (req_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create http request !");
        http_connection_release (con);
        op_data->getattr_cb (op_data->req, FALSE, 0, 0, 0, 0);
        en->is_updating = FALSE;
        g_free (op_data);
        return;
    }
}
*/

// return entry attributes
void dir_tree_getattr (DirTree *dtree, fuse_ino_t ino, 
    dir_tree_getattr_cb getattr_cb, fuse_req_t req)
{
    DirEntry *en;
    
    LOG_debug (DIR_TREE_LOG, INO_H"Getting attributes..", INO_T (ino));
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));
    
    // entry not found
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Entry (%"INO_FMT") not found !", INO ino);
        getattr_cb (req, FALSE, 0, 0, 0, 0);
        return;
    }

    // get extra info for file
    /*
    if (!en->is_updating) {
        GetAttrOpData *op_data;

        //XXX: CacheMng !

        op_data = g_new0 (GetAttrOpData, 1);
        op_data->dtree = dtree;
        op_data->getattr_cb = getattr_cb;
        op_data->req = req;
        op_data->ino = ino;

        en->is_updating = TRUE;

        if (!client_pool_get_client (application_get_ops_client_pool (dtree->app), dir_tree_getattr_on_con_cb, op_data)) {
            LOG_err (DIR_TREE_LOG, "Failed to get http client !");
            getattr_cb (req, FALSE, 0, 0, 0, 0);
            g_free (op_data);
        }

        return;
    }
    */

    getattr_cb (req, TRUE, en->ino, en->mode, en->size, en->ctime);
}
/*}}}*/

/*{{{ dir_tree_setattr */
// set entry's attributes
// update directory cache
// XXX: not fully implemented
void dir_tree_setattr (DirTree *dtree, fuse_ino_t ino, 
    G_GNUC_UNUSED struct stat *attr, G_GNUC_UNUSED int to_set,
    dir_tree_setattr_cb setattr_cb, fuse_req_t req, G_GNUC_UNUSED void *fi)
{
    DirEntry  *en;
    
    LOG_debug (DIR_TREE_LOG, "Setting attributes for %"INO_FMT, INO ino);
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));
    
    // entry not found
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Entry (%"INO_FMT") not found !", INO ino);
        setattr_cb (req, FALSE, 0, 0, 0);
        return;
    }
    //XXX: en->mode
    setattr_cb (req, TRUE, en->ino, en->mode, en->size);
}
/*}}}*/

/*{{{ dir_tree_file_create */

// add new file entry to directory
void dir_tree_file_create (DirTree *dtree, fuse_ino_t parent_ino, const char *name, mode_t mode,
    DirTree_file_create_cb file_create_cb, fuse_req_t req, struct fuse_file_info *fi)
{
    DirEntry *dir_en, *en;
    FileIO *fop;
    
    // get parent, must be dir
    dir_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    
    // entry not found
    if (!dir_en || dir_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "Directory (%"INO_FMT") not found !", INO parent_ino);
        file_create_cb (req, FALSE, 0, 0, 0, fi);
        return;
    }
    
    // check if such entry exists
    en = g_hash_table_lookup (dir_en->h_dir_tree, name);
    if (!en) {
        // create a new entry
        en = dir_tree_add_entry (dtree, name, mode, DET_file, parent_ino, 0, time (NULL));
        if (!en) {
            LOG_err (DIR_TREE_LOG, "Failed to create file: %s !", name);
            file_create_cb (req, FALSE, 0, 0, 0, fi);
            return;
        }
    } else {
        // update
        en->removed = FALSE;
        en->access_time = time (NULL);

        // inform the parent that his dir cache is no longer up-to-dated
        dir_tree_entry_modified (dtree, dir_en);
    }

    //XXX: set as new 
    en->is_modified = TRUE;

    fop = fileio_create (dtree->app, en->fullpath, en->ino, TRUE);
    fi->fh = (uint64_t) fop;

    LOG_debug (DIR_TREE_LOG, INO_FOP_H"New Entry created: %s, directory ino: %"INO_FMT, INO_T (en->ino), fop, name, INO parent_ino);

    file_create_cb (req, TRUE, en->ino, en->mode, en->size, fi);
}
/*}}}*/

/*{{{ dir_tree_file_open */
// existing file is opened, create context data
void dir_tree_file_open (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi, 
    DirTree_file_open_cb file_open_cb, fuse_req_t req)
{
    DirEntry *en;
    FileIO *fop;

    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO ino);
        file_open_cb (req, FALSE, fi);
        return;
    }

    fop = fileio_create (dtree->app, en->fullpath, en->ino, FALSE);
    fi->fh = (uint64_t) fop;

    LOG_debug (DIR_TREE_LOG, INO_FOP_H"dir_tree_open", INO_T (en->ino), fop);

    file_open_cb (req, TRUE, fi);
}
/*}}}*/

/*{{{ dir_tree_file_release*/
// file is closed, free context data
void dir_tree_file_release (DirTree *dtree, fuse_ino_t ino, G_GNUC_UNUSED struct fuse_file_info *fi)
{
    DirEntry *en;
    FileIO *fop;

    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_msg (DIR_TREE_LOG, INO_H"Entry not found !", INO_T (ino));
        //XXX
        return;
    }

    fop = (FileIO *)fi->fh;

    LOG_debug (DIR_TREE_LOG, INO_FOP_H"dir_tree_file_release", INO_T (ino), fop);

    fileio_release (fop);
}
/*}}}*/

/*{{{ dir_tree_file_read */

typedef struct {
    DirTree_file_read_cb file_read_cb;
    fuse_req_t req;
    size_t size;
    fuse_ino_t ino;
} FileReadOpData;

static void dir_tree_on_buffer_read_cb (gpointer ctx, gboolean success, char *buf, size_t size)
{
    FileReadOpData *op_data = (FileReadOpData *)ctx;

    LOG_debug (DIR_TREE_LOG, INO_FROP_H"file READ_cb !", INO_T (op_data->ino), op_data);

    if (!success) {
        LOG_err (DIR_TREE_LOG, INO_FROP_H"Failed to read file !", INO_T (op_data->ino), op_data);
        op_data->file_read_cb (op_data->req, FALSE, NULL, 0);
        g_free (op_data);
        return;
    }

    op_data->file_read_cb (op_data->req, TRUE, buf, size);
    g_free (op_data);
}

// read file starting at off position, size length
void dir_tree_file_read (DirTree *dtree, fuse_ino_t ino, 
    size_t size, off_t off,
    DirTree_file_read_cb file_read_cb, fuse_req_t req,
    G_GNUC_UNUSED struct fuse_file_info *fi)
{
    DirEntry *en;
    FileIO *fop;
    FileReadOpData *op_data;
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_err (DIR_TREE_LOG, INO_H"Entry not found !", INO_T (ino));
        file_read_cb (req, FALSE, NULL, 0);
        return;
    }
    
    fop = (FileIO *)fi->fh;

    LOG_debug (DIR_TREE_LOG, INO_FOP_H"Read inode, size: %zd, off: %"OFF_FMT, INO_T (ino), fop, size, off);
    
    op_data = g_new0 (FileReadOpData, 1);
    op_data->file_read_cb = file_read_cb;
    op_data->req = req;
    op_data->size = size;
    op_data->ino = ino;

    fileio_read_buffer (fop, size, off, ino, dir_tree_on_buffer_read_cb, op_data);
}
/*}}}*/

/*{{{ dir_tree_file_write */

typedef struct {
    DirTree *dtree;
    DirTree_file_write_cb file_write_cb;
    fuse_req_t req;
    fuse_ino_t ino;
    off_t off;
} FileWriteOpData;

// buffer is written into local file, or error
static void dir_tree_on_buffer_written_cb (FileIO *fop, gpointer ctx, gboolean success, size_t count)
{
    FileWriteOpData *op_data = (FileWriteOpData *) ctx;
    DirEntry *en;

    op_data->file_write_cb (op_data->req, success, count);

    LOG_debug (DIR_TREE_LOG, INO_FOP_H"Buffer written, count: %zu", INO_T (op_data->ino), fop, count);

    // we need to update entry size !
    if (success) {
        guint64 len;

        en = g_hash_table_lookup (op_data->dtree->h_inodes, GUINT_TO_POINTER (op_data->ino));
        if (!en) {
            LOG_msg (DIR_TREE_LOG, INO_H"Entry not found !", INO_T (op_data->ino));
            g_free (op_data);
            return;
        }
        
        // try to get file size from CacheMng
        len = cache_mng_get_file_length (application_get_cache_mng (op_data->dtree->app), op_data->ino);
        
        // calculate current size in case of CacheMng is disabled or the file is not stored in CacheMng
        if (len == 0) {
            len = op_data->off + count;
            LOG_debug (DIR_TREE_LOG, INO_H"Recalculating file size !", INO_T (op_data->ino));
        }

        en->size = len;
        //en->ctime = time (NULL);
    }
    
    g_free (op_data);
}

// send data via http client
void dir_tree_file_write (DirTree *dtree, fuse_ino_t ino, 
    const char *buf, size_t size, off_t off, 
    DirTree_file_write_cb file_write_cb, fuse_req_t req,
    struct fuse_file_info *fi)
{
    DirEntry *en;
    FileIO *fop;
    FileWriteOpData *op_data;

    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_msg (DIR_TREE_LOG, INO_H"Entry not found !", INO_T (ino));
        file_write_cb (req, FALSE,  0);
        return;
    }
    
    fop = (FileIO *)fi->fh;

    // set updated time for write op
    en->updated_time = time (NULL);
    
    LOG_debug (DIR_TREE_LOG, INO_FOP_H"write inode, size: %zd, off: %"OFF_FMT, INO_T (ino), fop, size, off);

    op_data = g_new0 (FileWriteOpData, 1);
    op_data->dtree = dtree;
    op_data->file_write_cb = file_write_cb;
    op_data->req = req;
    op_data->ino = ino;
    op_data->off = off;

    fileio_write_buffer (fop, buf, size, off, ino, dir_tree_on_buffer_written_cb, op_data);
}
/*}}}*/

/*{{{ dir_tree_file_remove */

typedef struct {
    DirTree *dtree;
    fuse_ino_t ino;
    DirTree_file_remove_cb file_remove_cb;
    fuse_req_t req;
} FileRemoveData;

// file is removed
static void dir_tree_file_remove_on_con_data_cb (HttpConnection *con, gpointer ctx, gboolean success,
    G_GNUC_UNUSED const gchar *buf, G_GNUC_UNUSED size_t buf_len, 
    G_GNUC_UNUSED struct evkeyvalq *headers)
{
    FileRemoveData *data = (FileRemoveData *) ctx;
    DirEntry *en;
    
    http_connection_release (con);
    
    en = g_hash_table_lookup (data->dtree->h_inodes, GUINT_TO_POINTER (data->ino));
    if (!en) {
        LOG_err (DIR_TREE_LOG, INO_H"Entry not found !", INO_T (data->ino));
        if (data->file_remove_cb)
            data->file_remove_cb (data->req, FALSE);
		g_free (data);
        return;
    }
   
    LOG_debug (DIR_TREE_LOG, INO_H"Entry is removed !", INO_T (data->ino));

    en->removed = TRUE;
    en->age = 0;

    dir_tree_entry_modified (data->dtree, en);

    if (data->file_remove_cb)
        data->file_remove_cb (data->req, success);

    g_free (data);
}

// http client is ready for a new request
static void dir_tree_file_remove_on_con_cb (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    FileRemoveData *data = (FileRemoveData *) ctx;
    gchar *req_path;
    gboolean res;
    DirEntry *en;
    
    en = g_hash_table_lookup (data->dtree->h_inodes, GUINT_TO_POINTER (data->ino));
    if (!en) {
        LOG_err (DIR_TREE_LOG, INO_H"Entry not found !", INO_T (data->ino));
        if (data->file_remove_cb)
            data->file_remove_cb (data->req, FALSE);
        g_free (data);
        return;
    }

    http_connection_acquire (con);

    req_path = g_strdup_printf ("/%s", en->fullpath);
    res = http_connection_make_request (con, 
        req_path, "DELETE", 
        NULL,
        dir_tree_file_remove_on_con_data_cb,
        data
    );
    g_free (req_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create http request !");
        data->file_remove_cb (data->req, FALSE);
        
        http_connection_release (con);
        g_free (data);
    }
}

// remove file
void dir_tree_file_remove (DirTree *dtree, fuse_ino_t ino, DirTree_file_remove_cb file_remove_cb, fuse_req_t req)
{
    DirEntry *en;
    FileRemoveData *data;
    
    LOG_debug (DIR_TREE_LOG, INO_H"Removing  inode", INO_T (ino));

    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO ino);
        file_remove_cb (req, FALSE);
        return;
    }

    if (en->type != DET_file) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") is not a file !", INO ino);
        file_remove_cb (req, FALSE);
        return;
    }

    // XXX: not sure if the best place
    cache_mng_remove_file (application_get_cache_mng (dtree->app), ino);

    data = g_new0 (FileRemoveData, 1);
    data->dtree = dtree;
    data->ino = ino;
    data->file_remove_cb = file_remove_cb;
    data->req = req;

    client_pool_get_client (application_get_ops_client_pool (dtree->app),
        dir_tree_file_remove_on_con_cb, data);
}

void dir_tree_file_unlink (DirTree *dtree, fuse_ino_t parent_ino, const char *name, 
    DirTree_file_remove_cb file_remove_cb, fuse_req_t req)
{
    DirEntry *en, *parent_en;
    
    LOG_debug (DIR_TREE_LOG, "Unlinking %s", name);

    parent_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    if (!parent_en) {
        LOG_err (DIR_TREE_LOG, "Parent not found: %"INO_FMT, INO parent_ino);
        file_remove_cb (req, FALSE);
        return;
    }

    en = g_hash_table_lookup (parent_en->h_dir_tree, name);
    if (!en) {
        LOG_err (DIR_TREE_LOG, "Parent not found: %"INO_FMT, INO parent_ino);
        file_remove_cb (req, FALSE);
        return;
    }

    dir_tree_file_remove (dtree, en->ino, file_remove_cb, req);
}

/*}}}*/

/*{{{ dir_tree_dir_remove */

typedef struct {
    DirTree *dtree;
    fuse_ino_t ino;
    DirTree_dir_remove_cb dir_remove_cb;
    fuse_req_t req;
    GQueue *q_objects_to_remove;
} DirRemoveData;

static void dir_tree_dir_remove_try_to_remove_object (HttpConnection *con, DirRemoveData *data);

// object is removed, call remove function again
static void dir_tree_dir_remove_on_object_removed_cb (HttpConnection *con, gpointer ctx, 
    G_GNUC_UNUSED gboolean success,
    G_GNUC_UNUSED const gchar *buf, G_GNUC_UNUSED size_t buf_len, 
    G_GNUC_UNUSED struct evkeyvalq *headers)
{
    DirRemoveData *data = (DirRemoveData *) ctx;

    dir_tree_dir_remove_try_to_remove_object (con, data);
}

// check if there is any object left in the queue and remove it
static void dir_tree_dir_remove_try_to_remove_object (HttpConnection *con, DirRemoveData *data)
{
    gchar *line;
    gchar *req_path;
    gboolean res;

    // check if all objects are removed
    if (g_queue_is_empty (data->q_objects_to_remove)) {
        DirEntry *en;
        
        LOG_debug (DIR_TREE_LOG, "All objects are removed !");
        http_connection_release (con);

        en = g_hash_table_lookup (data->dtree->h_inodes, GUINT_TO_POINTER (data->ino));
        if (!en) {
            LOG_err (DIR_TREE_LOG, INO_H"Entry not found !", INO_T (data->ino));
            if (data->dir_remove_cb)
                data->dir_remove_cb (data->req, FALSE);
        } else {
            // hide from the dir list
            en->age = 0;
            if (data->dir_remove_cb)
                data->dir_remove_cb (data->req, TRUE);
        }

        g_queue_free_full (data->q_objects_to_remove, g_free);
        g_free (data);
        return;
    }

    line = g_queue_pop_tail (data->q_objects_to_remove);

    req_path = g_strdup_printf ("/%s", line);
    g_free (line);

    res = http_connection_make_request (con, 
        req_path, "DELETE", 
        NULL,
        dir_tree_dir_remove_on_object_removed_cb,
        data
    );

    g_free (req_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create http request !");
        http_connection_release (con);
        if (data->dir_remove_cb)
            data->dir_remove_cb (data->req, FALSE);

        g_queue_free_full (data->q_objects_to_remove, g_free);
        g_free (data);
    }

}

// got the list of all objects in the directory
// create list to-remove
static void dir_tree_dir_remove_on_con_objects_cb (HttpConnection *con, gpointer ctx, gboolean success,
        const gchar *buf, size_t buf_len, G_GNUC_UNUSED struct evkeyvalq *headers)
{
    DirRemoveData *data = (DirRemoveData *) ctx;
    struct evbuffer *evb;
    char *line;

    if (!success) {
        LOG_err (DIR_TREE_LOG, "Failed to get directory's content !");
        http_connection_release (con);
        if (data->dir_remove_cb)
            data->dir_remove_cb (data->req, FALSE);
        g_free (data);
        return;
    }

    evb = evbuffer_new ();
    evbuffer_add (evb, buf, buf_len);

    data->q_objects_to_remove = g_queue_new ();
    while ((line = evbuffer_readln (evb, NULL, EVBUFFER_EOL_CRLF))) {
        LOG_debug (DIR_TREE_LOG, "Removing %s", line);
        g_queue_push_head (data->q_objects_to_remove, line);
    }

    evbuffer_free (evb);

    dir_tree_dir_remove_try_to_remove_object (con, data);

}

// http Connection is ready
static void dir_tree_dir_remove_on_con_cb (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    DirRemoveData *data = (DirRemoveData *) ctx;
    gchar *req_path;
    gboolean res;
    DirEntry *en;
    
    en = g_hash_table_lookup (data->dtree->h_inodes, GUINT_TO_POINTER (data->ino));
    if (!en) {
        LOG_err (DIR_TREE_LOG, INO_H"Entry not found !", INO_T (data->ino));
        if (data->dir_remove_cb)
            data->dir_remove_cb (data->req, FALSE);

        g_free (data);
        return;
    }

    // XXX: max keys
    req_path = g_strdup_printf ("?prefix=%s/", en->fullpath);

    http_connection_acquire (con);
    res = http_connection_make_request (con, 
        req_path, "GET", 
        NULL,
        dir_tree_dir_remove_on_con_objects_cb,
        data
    );

    g_free (req_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create http request !");
        if (data->dir_remove_cb)
            data->dir_remove_cb (data->req, FALSE);
        
        http_connection_release (con);
        g_free (data);
    }
}

// try to get directory entry
void dir_tree_dir_remove (DirTree *dtree, fuse_ino_t parent_ino, const char *name, 
    DirTree_dir_remove_cb dir_remove_cb, fuse_req_t req)
{
    DirRemoveData *data;
    DirEntry *parent_en;
    DirEntry *en;

    LOG_debug (DIR_TREE_LOG, "Removing dir: %s parent: %"INO_FMT, name, INO parent_ino);

    parent_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    if (!parent_en || parent_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO parent_ino);
        if (dir_remove_cb)
            dir_remove_cb (req, FALSE);
        return;
    }

    en = g_hash_table_lookup (parent_en->h_dir_tree, name);
    if (!en) {
        LOG_debug (DIR_TREE_LOG, "Entry '%s' not found !", name);
        if (dir_remove_cb)
            dir_remove_cb (req, FALSE);
        return;
    }
    
    // ok, directory is found, get HttpConnection
    data = g_new0 (DirRemoveData, 1);
    data->dtree = dtree;
    data->dir_remove_cb = dir_remove_cb;
    data->req = req;
    data->ino = en->ino;

    if (!client_pool_get_client (application_get_ops_client_pool (dtree->app),
        dir_tree_dir_remove_on_con_cb, data)) {
        LOG_debug (DIR_TREE_LOG, "Failed to get HTTPPool !");
        if (dir_remove_cb)
            dir_remove_cb (req, FALSE);
        g_free (data);
        return;
    }
}
/*}}}*/

/*{{{ dir_tree_dir_create */
void dir_tree_dir_create (DirTree *dtree, fuse_ino_t parent_ino, const char *name, mode_t mode,
     dir_tree_mkdir_cb mkdir_cb, fuse_req_t req)
{
    DirEntry *dir_en, *en;
    
    LOG_debug (DIR_TREE_LOG, "Creating dir: %s", name);
    
    dir_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    
    // entry not found
    if (!dir_en || dir_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "Directory (%"INO_FMT") not found !", INO parent_ino);
        mkdir_cb (req, FALSE, 0, 0, 0, 0);
        return;
    }
    
    en = g_hash_table_lookup (dir_en->h_dir_tree, name);
    if (!en) {
        // create a new entry
        en = dir_tree_add_entry (dtree, name, mode, DET_dir, parent_ino, 10, time (NULL));
        if (!en) {
            LOG_err (DIR_TREE_LOG, "Failed to create dir: %s !", name);
            mkdir_cb (req, FALSE, 0, 0, 0, 0);
            return;
        }
    } else {
        // lookup has created a default "file type" entry
        en->type = DET_dir;
        if (!en->h_dir_tree)
            en->h_dir_tree = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, dir_entry_destroy);
        en->removed = FALSE;
        en->access_time = time (NULL);
        if (en->dir_cache)
            g_free (en->dir_cache);
        en->dir_cache = NULL;
        en->dir_cache_size = 0;
        en->dir_cache_created = 0;
    }

    //XXX: set as new 
    en->is_modified = FALSE;
    // do not delete it
    en->age = G_MAXUINT32;
    en->mode = DIR_DEFAULT_MODE;

    mkdir_cb (req, TRUE, en->ino, en->mode, en->size, en->ctime);
}
/*}}}*/

/*{{{ dir_tree_rename */

typedef struct {
    DirTree *dtree;
    fuse_ino_t parent_ino;
    char *name;
    fuse_ino_t newparent_ino;
    char *newname;
    DirTree_rename_cb rename_cb;
    fuse_req_t req;
} RenameData;

static void rename_data_destroy (RenameData *rdata)
{
    g_free (rdata->name);
    g_free (rdata->newname);
    g_free (rdata);
}

/*{{{ delete object */

static void dir_tree_on_rename_delete_cb (HttpConnection *con, gpointer ctx, gboolean success,
    G_GNUC_UNUSED const gchar *buf, G_GNUC_UNUSED size_t buf_len, 
    G_GNUC_UNUSED struct evkeyvalq *headers)
{
    RenameData *rdata = (RenameData *) ctx;
    DirEntry *en;
    DirEntry *parent_en;
    DirEntry *newparent_en;
    
    http_connection_release (con);

    if (!success) {
        LOG_err (DIR_TREE_LOG, "Failed to rename !");
        if (rdata->rename_cb)
            rdata->rename_cb (rdata->req, FALSE);
        rename_data_destroy (rdata);
        return;
    }
    
    parent_en = g_hash_table_lookup (rdata->dtree->h_inodes, GUINT_TO_POINTER (rdata->parent_ino));
    if (!parent_en || parent_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO rdata->parent_ino);
        if (rdata->rename_cb)
            rdata->rename_cb (rdata->req, FALSE);
        rename_data_destroy (rdata);
        return;
    }
   
    en = g_hash_table_lookup (parent_en->h_dir_tree, rdata->name);
    if (!en) {
        LOG_debug (DIR_TREE_LOG, "Entry '%s' not found !", rdata->name);
        if (rdata->rename_cb)
            rdata->rename_cb (rdata->req, FALSE);
        rename_data_destroy (rdata);
        return;
    }
    
    newparent_en = g_hash_table_lookup (rdata->dtree->h_inodes, GUINT_TO_POINTER (rdata->newparent_ino));
    if (!newparent_en || newparent_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO rdata->newparent_ino);
        if (rdata->rename_cb)
            rdata->rename_cb (rdata->req, FALSE);
        rename_data_destroy (rdata);
        return;
    }

    // 1. inform that source was removed
    en->removed = TRUE;
    dir_tree_entry_modified (rdata->dtree, en);

    // 2. inform that desination dir was modified
    dir_tree_entry_modified (rdata->dtree, newparent_en);

    // done !
    if (rdata->rename_cb)
        rdata->rename_cb (rdata->req, TRUE);
    rename_data_destroy (rdata);
}

static void dir_tree_on_rename_delete_con_cb (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    RenameData *rdata = (RenameData *) ctx;
    gchar *req_path = NULL;
    gboolean res;
    DirEntry *en;
    DirEntry *parent_en;
    
    parent_en = g_hash_table_lookup (rdata->dtree->h_inodes, GUINT_TO_POINTER (rdata->parent_ino));
    if (!parent_en || parent_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO rdata->parent_ino);
        if (rdata->rename_cb)
            rdata->rename_cb (rdata->req, FALSE);
        rename_data_destroy (rdata);
        return;
    }
   
    en = g_hash_table_lookup (parent_en->h_dir_tree, rdata->name);
    if (!en) {
        LOG_debug (DIR_TREE_LOG, "Entry '%s' not found !", rdata->name);
        if (rdata->rename_cb)
            rdata->rename_cb (rdata->req, FALSE);
        rename_data_destroy (rdata);
        return;
    }

    http_connection_acquire (con);
    req_path = g_strdup_printf ("/%s", en->fullpath);
    res = http_connection_make_request (con, 
        req_path, "DELETE", 
        NULL,
        dir_tree_on_rename_delete_cb,
        rdata
    );
    g_free (req_path);
    
    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create http request !");
        if (rdata->rename_cb)
            rdata->rename_cb (rdata->req, FALSE);
        http_connection_release (con);
        rename_data_destroy (rdata);
        return;
    }
}
/*}}}*/

/*{{{ copy object */
static void dir_tree_on_rename_copy_cb (HttpConnection *con, gpointer ctx, gboolean success,
    G_GNUC_UNUSED const gchar *buf, G_GNUC_UNUSED size_t buf_len, 
    G_GNUC_UNUSED struct evkeyvalq *headers)
{
    RenameData *rdata = (RenameData *) ctx;
    DirEntry *newparent_en;
    DirEntry *en;
    
    http_connection_release (con);

    if (!success) {
        LOG_err (DIR_TREE_LOG, "Failed to rename !");
        if (rdata->rename_cb)
            rdata->rename_cb (rdata->req, FALSE);
        rename_data_destroy (rdata);
        return;
    }

    //XXX: a 200 OK response can contain either a success or an error

    // Update new entry
    newparent_en = g_hash_table_lookup (rdata->dtree->h_inodes, GUINT_TO_POINTER (rdata->newparent_ino));
    if (!newparent_en || newparent_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO rdata->newparent_ino);
        if (rdata->rename_cb)
            rdata->rename_cb (rdata->req, FALSE);
        rename_data_destroy (rdata);
        return;
    }

    en = g_hash_table_lookup (newparent_en->h_dir_tree, rdata->newname);
    if (!en) {
        LOG_debug (DIR_TREE_LOG, "Entry '%s' not found !", rdata->newname);
        if (rdata->rename_cb)
            rdata->rename_cb (rdata->req, FALSE);
        rename_data_destroy (rdata);
        return;
    }

    en->removed = FALSE;
    en->access_time = time (NULL);

    // inform the parent that his dir cache is no longer up-to-dated
    dir_tree_entry_modified (rdata->dtree, newparent_en);

    //XXX: reuse file_delete code
    if (!client_pool_get_client (application_get_ops_client_pool (rdata->dtree->app),
        dir_tree_on_rename_delete_con_cb, rdata)) {
        LOG_debug (DIR_TREE_LOG, "Failed to get HTTPPool !");
        if (rdata->rename_cb)
           rdata->rename_cb (rdata->req, FALSE);
        rename_data_destroy (rdata);
        return;
    }
}

static void dir_tree_on_rename_copy_con_cb (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    RenameData *rdata = (RenameData *) ctx;
    gchar *dst_path = NULL;
    gchar *src_path = NULL;
    gboolean res;
    DirEntry *en;
    DirEntry *parent_en;
    DirEntry *newparent_en;
    
    parent_en = g_hash_table_lookup (rdata->dtree->h_inodes, GUINT_TO_POINTER (rdata->parent_ino));
    if (!parent_en || parent_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO rdata->parent_ino);
        if (rdata->rename_cb)
            rdata->rename_cb (rdata->req, FALSE);
        rename_data_destroy (rdata);
        return;
    }
   
    en = g_hash_table_lookup (parent_en->h_dir_tree, rdata->name);
    if (!en) {
        LOG_debug (DIR_TREE_LOG, "Entry '%s' not found !", rdata->name);
        if (rdata->rename_cb)
            rdata->rename_cb (rdata->req, FALSE);
        rename_data_destroy (rdata);
        return;
    }

    newparent_en = g_hash_table_lookup (rdata->dtree->h_inodes, GUINT_TO_POINTER (rdata->newparent_ino));
    if (!newparent_en || newparent_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO rdata->newparent_ino);
        if (rdata->rename_cb)
            rdata->rename_cb (rdata->req, FALSE);
        rename_data_destroy (rdata);
        return;
    }

    http_connection_acquire (con);
    
    // source
    src_path = g_strdup_printf ("%s/%s", conf_get_string (application_get_conf (rdata->dtree->app), "s3.bucket_name"), en->fullpath);
    http_connection_add_output_header (con, "x-amz-copy-source", src_path);
    g_free (src_path);

    if (rdata->newparent_ino == FUSE_ROOT_ID)
        dst_path = g_strdup_printf ("%s/%s", newparent_en->fullpath, rdata->newname);
    else
        dst_path = g_strdup_printf ("/%s/%s", newparent_en->fullpath, rdata->newname);

    LOG_debug (DIR_TREE_LOG, INO_CON_H"Rename: coping %s to %s", INO_T (en->ino), con, en->fullpath, dst_path); 

    res = http_connection_make_request (con, 
        dst_path, "PUT", 
        NULL,
        dir_tree_on_rename_copy_cb,
        rdata
    );
    g_free (dst_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create http request !");
        if (rdata->rename_cb)
            rdata->rename_cb (rdata->req, FALSE);
        http_connection_release (con);
        rename_data_destroy (rdata);
        return;
    }
}
/*}}}*/

void dir_tree_rename (DirTree *dtree, 
    fuse_ino_t parent_ino, const char *name, fuse_ino_t newparent_ino, const char *newname,
    DirTree_rename_cb rename_cb, fuse_req_t req)
{
    RenameData *rdata;
    DirEntry *parent_en;
    DirEntry *newparent_en;
    DirEntry *en;

    LOG_debug (DIR_TREE_LOG, "Renaming: %s parent: %"INO_FMT" to %s parent: %"INO_FMT, 
        name, INO parent_ino, newname, INO newparent_ino);

    parent_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    if (!parent_en || parent_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO parent_ino);
        if (rename_cb)
            rename_cb (req, FALSE);
        return;
    }

    newparent_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (newparent_ino));
    if (!newparent_en || newparent_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO newparent_ino);
        if (rename_cb)
            rename_cb (req, FALSE);
        return;
    }

    en = g_hash_table_lookup (parent_en->h_dir_tree, name);
    if (!en) {
        LOG_debug (DIR_TREE_LOG, "Entry '%s' not found !", name);
        if (rename_cb)
            rename_cb (req, FALSE);
        return;
    }
    
    // we need to rename each object, which contains this directory in the path
    // could take a quite amount of time
    if (en->type == DET_dir) {
        LOG_err (DIR_TREE_LOG, "Removing directories is not supported !");
        if (rename_cb)
            rename_cb (req, FALSE);
        return;
    }

    // You create a copy of your object up to 5 GB in size in a single atomic operation using this API. 
    // However, for copying an object greater than 5 GB, you must use the multipart upload API
    if (en->size >= FIVEG) {
        LOG_err (DIR_TREE_LOG, "Removing files larger than 5Gb is not currently supported !");
        if (rename_cb)
            rename_cb (req, FALSE);
        return;
    }

    rdata = g_new0 (RenameData, 1);
    rdata->dtree = dtree;
    rdata->parent_ino = parent_ino;
    rdata->name = g_strdup (name);
    rdata->newparent_ino = newparent_ino;
    rdata->newname = g_strdup (newname);
    rdata->rename_cb = rename_cb;
    rdata->req = req;
    
    if (!client_pool_get_client (application_get_ops_client_pool (dtree->app),
        dir_tree_on_rename_copy_con_cb, rdata)) {
        LOG_debug (DIR_TREE_LOG, "Failed to get HTTPPool !");
        if (rename_cb)
           rename_cb (req, FALSE);
        rename_data_destroy (rdata);
        return;
    }
}
/*}}}*/

/*{{{ dir_tree_getxattr */
typedef enum {
    XATR_etag = 0,
    XATR_version = 1,
    XATR_content = 2,
} XAttrType;

typedef struct {
    DirTree *dtree;
    fuse_ino_t ino;
    fuse_req_t req;
    dir_tree_getxattr_cb getxattr_cb;
    size_t size;
    XAttrType attr_type;
} XAttrData;

static const gchar *dir_tree_getxattr_from_entry (DirEntry *en, XAttrType attr_type)
{
    gchar *out = NULL;
    
    if (attr_type == XATR_etag) {
        out = en->etag;
    } else if (attr_type == XATR_version) {
        out = en->version_id;
    } else if (attr_type == XATR_content) {
        out = en->content_type;
    }

    return out;
}

void dir_tree_entry_update_xattrs (DirEntry *en, struct evkeyvalq *headers)
{
    const gchar *header = NULL;
    
    // For objects created by the PUT Object operation and the POST Object operation, 
    // the ETag is a quoted, 32-digit hexadecimal string representing the MD5 digest of the object data. 
    // For other objects, the ETag may or may not be an MD5 digest of the object data
    header = http_find_header (headers, "ETag");
    if (header) {
        gchar *tmp;
        tmp = (gchar *)header;
        tmp = str_remove_quotes (tmp);

        if (!en->etag)
            en->etag = g_strdup (tmp);
        else if (strcmp (en->etag, tmp)) {
            g_free (en->etag);
            en->etag = g_strdup (tmp);
        }
    }

    header = http_find_header (headers, "x-amz-version-id");
    if (header) {
        if (!en->version_id)
            en->version_id = g_strdup (header);
        else if (strcmp (en->version_id, header)) {
            g_free (en->version_id);
            en->version_id = g_strdup (header);
        }
    }

    header = http_find_header (headers, "Content-Type");
    if (header) {
        if (!en->content_type)
            en->content_type = g_strdup (header);
        else if (strcmp (en->content_type, header)) {
            g_free (en->content_type);
            en->content_type = g_strdup (header);
        }
    }

    en->xattr_time = time (NULL);
}

static void dir_tree_on_getxattr_cb (HttpConnection *con, void *ctx, gboolean success,
    G_GNUC_UNUSED const gchar *buf, G_GNUC_UNUSED size_t buf_len, 
    struct evkeyvalq *headers)
{
    XAttrData *xattr_data = (XAttrData *) ctx;
    DirEntry *en;
    
    LOG_debug (DIR_TREE_LOG, INO_H"Got Xattributes !", INO_T (xattr_data->ino));

    // release HttpConnection
    http_connection_release (con);

    // file not found
    if (!success) {
        LOG_err (DIR_TREE_LOG, INO_H"Failed to get Xattributes !", INO_T (xattr_data->ino));
        xattr_data->getxattr_cb (xattr_data->req, FALSE, xattr_data->ino, NULL, 0);
        g_free (xattr_data);

        return;
    }

    en = g_hash_table_lookup (xattr_data->dtree->h_inodes, GUINT_TO_POINTER (xattr_data->ino));
    if (!en) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO xattr_data->ino);
        xattr_data->getxattr_cb (xattr_data->req, FALSE, xattr_data->ino, NULL, 0);
        g_free (xattr_data);
        return;
    }

    dir_tree_entry_update_xattrs (en, headers);

    xattr_data->getxattr_cb (xattr_data->req, TRUE, xattr_data->ino, 
        dir_tree_getxattr_from_entry (en, xattr_data->attr_type), xattr_data->size);
    
    g_free (xattr_data);
}

static void dir_tree_on_getxattr_con_cb (gpointer client, gpointer ctx)
{
    HttpConnection *con = (HttpConnection *) client;
    XAttrData *xattr_data = (XAttrData *) ctx;
    DirEntry *en;
    gchar *req_path = NULL;
    gboolean res;
    
    en = g_hash_table_lookup (xattr_data->dtree->h_inodes, GUINT_TO_POINTER (xattr_data->ino));
    if (!en) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO xattr_data->ino);
        xattr_data->getxattr_cb (xattr_data->req, FALSE, xattr_data->ino, NULL, 0);
        g_free (xattr_data);
        return;
    }
   
    http_connection_acquire (con);
    
    req_path = g_strdup_printf ("/%s", en->fullpath);

    res = http_connection_make_request (con, 
        req_path, "HEAD", NULL,
        dir_tree_on_getxattr_cb,
        xattr_data
    );

    g_free (req_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create http request !");
        http_connection_release (con);
        xattr_data->getxattr_cb (xattr_data->req, FALSE, xattr_data->ino, NULL, 0);
        g_free (xattr_data);
        return;
    }
}

void dir_tree_getxattr (DirTree *dtree, fuse_ino_t ino, 
    const char *name, size_t size,
    dir_tree_getxattr_cb getxattr_cb, fuse_req_t req)
{
    DirEntry *en;
    XAttrData *xattr_data = NULL;
    XAttrType attr_type;
    time_t t;
    
    LOG_debug (DIR_TREE_LOG, INO_H"Getting Xattributes ..", INO_T (ino));
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));
    
    // entry not found
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Entry (%"INO_FMT") not found !", INO ino);
        getxattr_cb (req, FALSE, ino, NULL, 0);
        return;
    }
    // Xattr for directories not supported
    if (en->type == DET_dir) {
        LOG_debug (DIR_TREE_LOG, "Xattr for directories not supported!");
        getxattr_cb (req, FALSE, ino, NULL, 0);
        return;
    }

    if (!strcmp (name, "user.version")) {
        attr_type = XATR_version;
    } else if (!strcmp (name, "user.etag") || !strcmp (name, "user.md5")) {
        attr_type = XATR_etag;
    } else if (!strcmp (name, "user.content_type")) {
        attr_type = XATR_content;
    } else {
        LOG_debug (DIR_TREE_LOG, "Xattr :%s not supported!", name);
        getxattr_cb (req, FALSE, ino, NULL, 0);
        return;
    }

    // check if we can get data from cache
    t = time (NULL);
    if (t >= en->xattr_time &&
        t - en->xattr_time >= (time_t)conf_get_uint (application_get_conf (dtree->app), "filesystem.dir_cache_max_time")) {

        xattr_data = g_new0 (XAttrData, 1);
        xattr_data->dtree = dtree;
        xattr_data->ino = ino;
        xattr_data->req = req;
        xattr_data->getxattr_cb = getxattr_cb;
        xattr_data->size = size;
        xattr_data->attr_type = attr_type;

        if (!client_pool_get_client (application_get_ops_client_pool (dtree->app),
            dir_tree_on_getxattr_con_cb, xattr_data)) {
            LOG_debug (DIR_TREE_LOG, "Failed to get HTTPPool !");
            getxattr_cb (req, FALSE, ino, NULL, 0);
            g_free (xattr_data);
            return;
        }

    // return from cache
    } else {
        getxattr_cb (req, TRUE, ino, dir_tree_getxattr_from_entry (en, attr_type), size);
    }
}
/*}}}*/

/*{{{ get_stats */
void dir_tree_get_stats (DirTree *dtree, guint32 *total_inodes, guint32 *file_num, guint32 *dir_num)
{
    GHashTableIter iter;
    DirEntry *entry;
    gpointer value;

    if (!dtree)
        return;
    
    *total_inodes = g_hash_table_size (dtree->h_inodes);
    *file_num = 0;
    *dir_num = 0;
    
    g_hash_table_iter_init (&iter, dtree->h_inodes);
    while (g_hash_table_iter_next (&iter, NULL, &value)) {
        entry = (DirEntry *) value;

        if (entry->type == DET_file) {
            *file_num = *file_num + 1;
        } else {
            *dir_num = *dir_num + 1;
        }
    }
}
/*}}}*/
