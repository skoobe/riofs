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
#include "s3fuse.h"
#include "s3http_connection.h"
#include "s3http_client.h"
#include "s3client_pool.h"
#include "file_io_ops.h"
#include "cache_mng.h"

/*{{{ struct / defines*/

struct _DirEntry {
    fuse_ino_t ino;
    fuse_ino_t parent_ino;
    gchar *basename;
    gchar *fullpath;
    guint64 age;
    gboolean removed;
    
    // type of directory entry
    DirEntryType type;

    gboolean is_modified; // do not show it

    off_t size;
    mode_t mode;
    time_t ctime;

    // for type == DET_dir
    char *dir_cache; // FUSE directory cache
    size_t dir_cache_size; // directory cache size
    time_t dir_cache_created;

    GHashTable *h_dir_tree; // name -> data

    FileIO *fop; // file operation object
    gboolean is_updating; // TRUE if getting attributes
};

struct _DirTree {
    DirEntry *root;
    GHashTable *h_inodes; // inode -> DirEntry
    Application *app;
    ConfData *conf;

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
    dtree->conf = application_get_conf (app);
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
    en->fop = NULL;
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

    // cache is empty
    en->dir_cache = NULL;
    en->dir_cache_size = 0;
    en->dir_cache_created = 0;

    LOG_debug (DIR_TREE_LOG, "Creating new DirEntry: %s, inode: %"INO_FMT", fullpath: %s, mode: %d", 
        en->basename, INO en->ino, en->fullpath, en->mode);
    
    if (type == DET_dir) {
        en->h_dir_tree = g_hash_table_new_full (g_str_hash, g_str_equal, NULL, dir_entry_destroy);
    }
    
    // add to global inode hash
    g_hash_table_insert (dtree->h_inodes, GUINT_TO_POINTER (en->ino), en);

    // add to the parent's hash
    if (parent_ino)
        g_hash_table_insert (parent_en->h_dir_tree, en->basename, en);

    return en;
}

// increase the age of directory
void dir_tree_start_update (DirTree *dtree, const gchar *dir_path)
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

    if (en->age < dtree->current_age && !en->is_modified) {
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
    
    g_hash_table_foreach_remove (parent_en->h_dir_tree, dir_tree_stop_update_on_remove_child_cb, dtree);
}

DirEntry *dir_tree_update_entry (DirTree *dtree, const gchar *path, DirEntryType type, 
    fuse_ino_t parent_ino, const gchar *entry_name, long long size, time_t last_modified)
{
    DirEntry *parent_en;
    DirEntry *en;

    LOG_debug (DIR_TREE_LOG, "Updating %s %ld", entry_name, size);
    
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
    } else {
        mode_t mode;

        if (type == DET_file)
            mode = FILE_DEFAULT_MODE;
        else
            mode = DIR_DEFAULT_MODE;
            
        en = dir_tree_add_entry (dtree, entry_name, mode,
            type, parent_ino, size, last_modified);
    }

    return en;
}

// let it know that directory cache have to be updated
static void dir_tree_entry_modified (DirTree *dtree, DirEntry *en)
{
    if (en->type == DET_dir) {
        if (en->dir_cache_size) {
            g_free (en->dir_cache);
            en->dir_cache = NULL;
            en->dir_cache_size = 0;
            en->dir_cache_created = 0;
        }
    } else {
        DirEntry *parent_en;
        
        parent_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (en->parent_ino));
        if (!parent_en) {
            LOG_err (DIR_TREE_LOG, "Parent not found for ino: %"INO_FMT" !", INO en->ino);
            return;
        }

        if (parent_en->dir_cache_size) {
            if (parent_en->dir_cache)
                g_free (parent_en->dir_cache);
            parent_en->dir_cache = NULL;
            parent_en->dir_cache_size = 0;
            parent_en->dir_cache_created = 0;
        }
        
        // XXX: get parent, update dir cache
    }
}
/*}}}*/

/*{{{ dir_tree_fill_dir_buf */

typedef struct {
    DirTree *dtree;
    fuse_ino_t ino;
    size_t size;
    off_t off;
    dir_tree_readdir_cb readdir_cb;
    fuse_req_t req;
    DirEntry *en;
} DirTreeFillDirData;

// callback: directory structure
void dir_tree_fill_on_dir_buf_cb (gpointer callback_data, gboolean success)
{
    DirTreeFillDirData *dir_fill_data = (DirTreeFillDirData *) callback_data;
    
    LOG_debug (DIR_TREE_LOG, "Dir fill callback: %s", success ? "SUCCESS" : "FAILED");

    if (!success) {
        dir_fill_data->readdir_cb (dir_fill_data->req, FALSE, dir_fill_data->size, dir_fill_data->off, NULL, 0);
    } else {
        struct dirbuf b; // directory buffer
        GHashTableIter iter;
        gpointer value;

        // construct directory buffer
        // add "." and ".."
        memset (&b, 0, sizeof(b));
        s3fuse_add_dirbuf (dir_fill_data->req, &b, ".", dir_fill_data->en->ino, 0);
        s3fuse_add_dirbuf (dir_fill_data->req, &b, "..", dir_fill_data->en->ino, 0);

        LOG_debug (DIR_TREE_LOG, "Entries in directory : %u", g_hash_table_size (dir_fill_data->en->h_dir_tree));
        
        // get all directory items
        g_hash_table_iter_init (&iter, dir_fill_data->en->h_dir_tree);
        while (g_hash_table_iter_next (&iter, NULL, &value)) {
            DirEntry *tmp_en = (DirEntry *) value;
            // add only updated entries
            if (tmp_en->age >= dir_fill_data->dtree->current_age)
                s3fuse_add_dirbuf (dir_fill_data->req, &b, tmp_en->basename, tmp_en->ino, tmp_en->size);
        }
        // done, save as cache
        dir_fill_data->en->dir_cache_size = b.size;
        dir_fill_data->en->dir_cache = g_malloc (b.size);
        dir_fill_data->en->dir_cache_created = time (NULL);


        memcpy (dir_fill_data->en->dir_cache, b.p, b.size);
        // send buffer to fuse
        dir_fill_data->readdir_cb (dir_fill_data->req, TRUE, dir_fill_data->size, dir_fill_data->off, b.p, b.size);

        //free buffer
        g_free (b.p);
    }

    g_free (dir_fill_data);
}

static void dir_tree_fill_dir_on_s3http_ready (gpointer client, gpointer ctx)
{
    S3HttpConnection *con = (S3HttpConnection *) client;
    DirTreeFillDirData *dir_fill_data = (DirTreeFillDirData *) ctx;

    //send s3http request
    s3http_connection_get_directory_listing (con, 
        dir_fill_data->en->fullpath, dir_fill_data->ino,
        dir_tree_fill_on_dir_buf_cb, dir_fill_data
    );
}

// return directory buffer from the cache
// or regenerate directory cache
void dir_tree_fill_dir_buf (DirTree *dtree, 
        fuse_ino_t ino, size_t size, off_t off,
        dir_tree_readdir_cb readdir_cb, fuse_req_t req)
{
    DirEntry *en;
    DirTreeFillDirData *dir_fill_data;
    time_t t;
    
    LOG_debug (DIR_TREE_LOG, "Requesting directory buffer for dir ino %"INO_FMT", size: %zd, off: %"OFF_FMT, INO ino, size, off);
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if directory does not exist
    // or it's not a directory type ?
    if (!en || en->type != DET_dir) {
        LOG_msg (DIR_TREE_LOG, "Directory (ino = %"INO_FMT") not found !", INO ino);
        readdir_cb (req, FALSE, size, off, NULL, 0);
        return;
    }
    
    t = time (NULL);

    // already have directory buffer in the cache
    if (en->dir_cache_size && t >= en->dir_cache_created && t - en->dir_cache_created <= conf_get_uint (dtree->conf, "filesystem.dir_cache_max_time")) {
        LOG_debug (DIR_TREE_LOG, "Sending directory buffer (ino = %"INO_FMT") from cache !", INO ino);
        readdir_cb (req, TRUE, size, off, en->dir_cache, en->dir_cache_size);
        return;
    }

    LOG_debug (DIR_TREE_LOG, "cache time: %ld  now: %ld", en->dir_cache_created, t);
    
    // reset dir cache
    if (en->dir_cache)
        g_free (en->dir_cache);
    en->dir_cache_size = 0;
    en->dir_cache_created = 0;

    dir_fill_data = g_new0 (DirTreeFillDirData, 1);
    dir_fill_data->dtree = dtree;
    dir_fill_data->ino = ino;
    dir_fill_data->size = size;
    dir_fill_data->off = off;
    dir_fill_data->readdir_cb = readdir_cb;
    dir_fill_data->req = req;
    dir_fill_data->en = en;

    if (!s3client_pool_get_client (application_get_ops_client_pool (dtree->app), dir_tree_fill_dir_on_s3http_ready, dir_fill_data)) {
        LOG_err (DIR_TREE_LOG, "Failed to get s3http client !");
        readdir_cb (req, FALSE, size, off, NULL, 0);
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

static void dir_tree_lookup_on_attr_cb (S3HttpConnection *con, void *ctx, gboolean success,
    const gchar *buf, size_t buf_len, 
    struct evkeyvalq *headers)
{
    LookupOpData *op_data = (LookupOpData *) ctx;
    const unsigned char *size_header;
    const unsigned char *meta_header;
    DirEntry  *en;
    
    LOG_debug (DIR_TREE_LOG, "Got attributes for ino: %"INO_FMT, INO op_data->ino);

    // release S3HttpConnection
    s3http_connection_release (con);

    en = g_hash_table_lookup (op_data->dtree->h_inodes, GUINT_TO_POINTER (op_data->ino));
    
    // entry not found
    if (!en) {
        LOG_err (DIR_TREE_LOG, "Entry (%"INO_FMT") not found !", INO op_data->ino);
        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data);
        return;
    }

    if (!success) {
        LOG_err (DIR_TREE_LOG, "Failed to get entry (%"INO_FMT") attributes !",  INO op_data->ino);
        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data);
        en->is_updating = FALSE;
        return;
    }

    // get Content-Length header
    size_header = evhttp_find_header (headers, "Content-Length");
    if (size_header) {
        en->size = strtoll ((char *)size_header, NULL, 10);
    }

    // is Meta-Size found - use it's value as the size of object
    size_header = evhttp_find_header (headers, "X-Object-Meta-Size");
    if (size_header) {
        en->size = strtoll ((char *)size_header, NULL, 10);
    }
    
    op_data->lookup_cb (op_data->req, TRUE, en->ino, en->mode, en->size, en->ctime);
    en->is_updating = FALSE;
    g_free (op_data);
}


//send s3http HEAD request
static void dir_tree_lookup_on_con_cb (gpointer client, gpointer ctx)
{
    S3HttpConnection *con = (S3HttpConnection *) client;
    LookupOpData *op_data = (LookupOpData *) ctx;
    gchar *req_path = NULL;
    gboolean res;
    DirEntry  *en;

    en = g_hash_table_lookup (op_data->dtree->h_inodes, GUINT_TO_POINTER (op_data->ino));
    
    // entry not found
    if (!en) {
        LOG_err (DIR_TREE_LOG, "Entry (%"INO_FMT") not found !", INO op_data->ino);
        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data);
        return;
    }

    s3http_connection_acquire (con);

    req_path = g_strdup_printf ("/%s", en->fullpath);

    res = s3http_connection_make_request (con, 
        req_path, "HEAD", NULL,
        dir_tree_lookup_on_attr_cb,
        op_data
    );

    g_free (req_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create s3http request !");
        s3http_connection_release (con);
        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        en->is_updating = FALSE;
        g_free (op_data);
        return;
    }
}

static void dir_tree_lookup_on_not_found_data_cb (S3HttpConnection *con, void *ctx, gboolean success,
    const gchar *buf, size_t buf_len, 
    struct evkeyvalq *headers)
{
    LookupOpData *op_data = (LookupOpData *) ctx;
    const unsigned char *size_header;
    const unsigned char *meta_header;
    const unsigned char *last_modified_header;
    DirEntry *en;
    time_t last_modified = time (NULL);
    DirEntry *parent_en;
    long long size = 0;
    
    LOG_debug (DIR_TREE_LOG, "Got attributes for ino: %"INO_FMT, INO op_data->ino);

    // release S3HttpConnection
    s3http_connection_release (con);

    // file not found
    if (!success) {
        LOG_err (DIR_TREE_LOG, "FileEntry not found %s", op_data->name);

        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data->name);
        g_free (op_data);
        return;
    }

    parent_en = g_hash_table_lookup (op_data->dtree->h_inodes, GUINT_TO_POINTER (op_data->parent_ino));
    if (!parent_en) {
        LOG_err (DIR_TREE_LOG, "Parent not found for ino: %"INO_FMT" !", INO op_data->parent_ino);

        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data->name);
        g_free (op_data);
        return;
    }

    // get Content-Length header
    size_header = evhttp_find_header (headers, "Content-Length");
    if (size_header) {
        size = strtoll ((char *)size_header, NULL, 10);
    }

    last_modified_header = evhttp_find_header (headers, "Last-Modified");
    if (last_modified_header) {
        struct tm tmp;
        strptime (last_modified_header, "%FT%T", &tmp);
        last_modified = mktime (&tmp);
    }

    en = dir_tree_update_entry (op_data->dtree, parent_en->fullpath, DET_file, 
        op_data->parent_ino, op_data->name, size, last_modified);

    if (!en) {
        LOG_err (DIR_TREE_LOG, "Failed to create FileEntry parent ino: %"INO_FMT" !", INO op_data->parent_ino);

        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data->name);
        g_free (op_data);
        return;
    }

    op_data->lookup_cb (op_data->req, TRUE, en->ino, en->mode, en->size, en->ctime);
    g_free (op_data->name);
    g_free (op_data);
}

//send s3http HEAD request when a file "not found"
static void dir_tree_lookup_on_not_found_con_cb (gpointer client, gpointer ctx)
{
    S3HttpConnection *con = (S3HttpConnection *) client;
    LookupOpData *op_data = (LookupOpData *) ctx;
    gchar *req_path = NULL;
    gboolean res;
    DirEntry *parent_en;
    gchar *fullpath;

    parent_en = g_hash_table_lookup (op_data->dtree->h_inodes, GUINT_TO_POINTER (op_data->parent_ino));
    if (!parent_en) {
        LOG_err (DIR_TREE_LOG, "Parent not found for ino: %"INO_FMT" !", INO op_data->parent_ino);

        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data->name);
        g_free (op_data);
        return;
    }

    s3http_connection_acquire (con);

    if (op_data->parent_ino == FUSE_ROOT_ID)
        fullpath = g_strdup_printf ("%s", op_data->name);
    else
        fullpath = g_strdup_printf ("%s/%s", parent_en->fullpath, op_data->name);

    req_path = g_strdup_printf ("/%s", fullpath);

    g_free (fullpath);

    res = s3http_connection_make_request (con, 
        req_path, "HEAD", NULL,
        dir_tree_lookup_on_not_found_data_cb,
        op_data
    );

    g_free (req_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create s3http request !");
        s3http_connection_release (con);
        op_data->lookup_cb (op_data->req, FALSE, 0, 0, 0, 0);
        g_free (op_data->name);
        g_free (op_data);
        return;
    }
}

// lookup entry and return attributes
void dir_tree_lookup (DirTree *dtree, fuse_ino_t parent_ino, const char *name,
    dir_tree_lookup_cb lookup_cb, fuse_req_t req)
{
    DirEntry *dir_en, *en;
    
    LOG_debug (DIR_TREE_LOG, "Looking up for '%s' in directory ino: %"INO_FMT, name, INO parent_ino);
    
    dir_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    
    // entry not found
    if (!dir_en || dir_en->type != DET_dir) {
        LOG_msg (DIR_TREE_LOG, "Directory not found ! ino: %"INO_FMT, INO parent_ino);
        lookup_cb (req, FALSE, 0, 0, 0, 0);
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

        LOG_debug (DIR_TREE_LOG, "Entry not found, sending request to storage server, name: %s", name);

        if (!s3client_pool_get_client (application_get_ops_client_pool (dtree->app), dir_tree_lookup_on_not_found_con_cb, op_data)) {
            LOG_err (DIR_TREE_LOG, "Failed to get s3http client !");
            lookup_cb (req, FALSE, 0, 0, 0, 0);
            g_free (op_data->name);
            g_free (op_data);
        }
        return;
    }
    
    // file is removed
    if (en->removed) {
        LOG_debug (DIR_TREE_LOG, "Entry '%s' is removed !", name);
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

        if (!s3client_pool_get_client (application_get_ops_client_pool (dtree->app), dir_tree_lookup_on_con_cb, op_data)) {
            LOG_err (DIR_TREE_LOG, "Failed to get s3http client !");
            lookup_cb (req, FALSE, 0, 0, 0, 0);
            en->is_updating = FALSE;
            g_free (op_data);
        }

        return;
    }
    */

    // hide it
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

        LOG_debug (DIR_TREE_LOG, "Entry '%s' is modified !", name);
        
        
        if (!s3client_pool_get_client (application_get_ops_client_pool (dtree->app), dir_tree_lookup_on_con_cb, op_data)) {
            LOG_err (DIR_TREE_LOG, "Failed to get s3http client !");
            lookup_cb (req, FALSE, 0, 0, 0, 0);
            en->is_updating = FALSE;
            g_free (op_data);
        }
        
        // lookup_cb (req, TRUE, en->ino, en->mode, en->size, en->ctime);
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

static void dir_tree_getattr_on_attr_cb (S3HttpConnection *con, void *ctx, gboolean success,
    const gchar *buf, size_t buf_len, 
    struct evkeyvalq *headers)
{
    GetAttrOpData *op_data = (GetAttrOpData *) ctx;
    const unsigned char *size_header;
    DirEntry  *en;
    

    // release S3HttpConnection
    s3http_connection_release (con);

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

    // get X-Object-Meta-Size header
    size_header = evhttp_find_header (headers, "X-Object-Meta-Size");
    if (size_header) {
        //XXX: CacheMng
        en->size = strtoll ((char *)size_header, NULL, 10);
    }

    // get X-Container-Bytes-Used header
    size_header = evhttp_find_header (headers, "X-Container-Bytes-Used");
    if (size_header) {
        //XXX: CacheMng
        en->size = strtoll ((char *)size_header, NULL, 10);
    }
    
    LOG_debug (DIR_TREE_LOG, "Got attributes for ino: %"INO_FMT" size: %zu", INO op_data->ino, en->size);
    
    en->is_updating = FALSE;
    op_data->getattr_cb (op_data->req, TRUE, en->ino, en->mode, en->size, en->ctime);
    g_free (op_data);
}


//send s3http HEAD request
static void dir_tree_getattr_on_con_cb (gpointer client, gpointer ctx)
{
    S3HttpConnection *con = (S3HttpConnection *) client;
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

    s3http_connection_acquire (con);

    req_path = g_strdup_printf ("/%s", en->fullpath);

    res = s3http_connection_make_request (con, 
        req_path, "HEAD", NULL,
        dir_tree_getattr_on_attr_cb,
        op_data
    );

    g_free (req_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create s3http request !");
        s3http_connection_release (con);
        op_data->getattr_cb (op_data->req, FALSE, 0, 0, 0, 0);
        en->is_updating = FALSE;
        g_free (op_data);
        return;
    }
}

// return entry attributes
void dir_tree_getattr (DirTree *dtree, fuse_ino_t ino, 
    dir_tree_getattr_cb getattr_cb, fuse_req_t req)
{
    DirEntry  *en;
    
    LOG_debug (DIR_TREE_LOG, "Getting attributes for %"INO_FMT, INO ino);
    
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

        if (!s3client_pool_get_client (application_get_ops_client_pool (dtree->app), dir_tree_getattr_on_con_cb, op_data)) {
            LOG_err (DIR_TREE_LOG, "Failed to get s3http client !");
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
void dir_tree_setattr (DirTree *dtree, fuse_ino_t ino, 
    struct stat *attr, int to_set,
    dir_tree_setattr_cb setattr_cb, fuse_req_t req, void *fi)
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
    
    // create a new entry
    en = dir_tree_add_entry (dtree, name, mode, DET_file, parent_ino, 0, time (NULL));
    if (!en) {
        LOG_err (DIR_TREE_LOG, "Failed to create file: %s !", name);
        file_create_cb (req, FALSE, 0, 0, 0, fi);
        return;
    }
    //XXX: set as new 
    en->is_modified = TRUE;

    fop = fileio_create (dtree->app, en->fullpath);
    en->fop = fop;

    LOG_debug (DIR_TREE_LOG, "[fop: %p] create %s, directory ino: %"INO_FMT, fop, name, INO parent_ino);

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

    fop = fileio_create (dtree->app, en->fullpath);
    en->fop = fop;

    LOG_debug (DIR_TREE_LOG, "[fop: %p] dir_tree_open inode %"INO_FMT, fop, INO ino);

    file_open_cb (req, TRUE, fi);
}
/*}}}*/

/*{{{ dir_tree_file_release*/
// file is closed, free context data
void dir_tree_file_release (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi)
{
    DirEntry *en;
    FileIO *fop;

    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO ino);
        //XXX
        return;
    }

    fop = en->fop;

    LOG_debug (DIR_TREE_LOG, "[fop: %p] dir_tree_file_release inode: %"INO_FMT, fop, INO ino);

    fileio_release (fop);
}
/*}}}*/

/*{{{ dir_tree_file_read */

typedef struct {
    DirTree_file_read_cb file_read_cb;
    fuse_req_t req;
    size_t size;
} FileReadOpData;

static void dir_tree_on_buffer_read_cb (gpointer ctx, gboolean success, char *buf, size_t size)
{
    FileReadOpData *op_data = (FileReadOpData *)ctx;

    LOG_debug (DIR_TREE_LOG, "file READ_cb !");

    if (!success) {
        LOG_err (DIR_TREE_LOG, "Failed to read file !");
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
    struct fuse_file_info *fi)
{
    DirEntry *en;
    char full_name[1024];
    FileIO *fop;
    FileReadOpData *op_data;
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO ino);
        file_read_cb (req, FALSE, NULL, 0);
        return;
    }
    
    fop = en->fop;

    LOG_debug (DIR_TREE_LOG, "[fop: %p] read inode %"INO_FMT", size: %zd, off: %"OFF_FMT, fop, INO ino, size, off);
    
    op_data = g_new0 (FileReadOpData, 1);
    op_data->file_read_cb = file_read_cb;
    op_data->req = req;
    op_data->size = size;

    fileio_read_buffer (fop, size, off, ino, dir_tree_on_buffer_read_cb, op_data);
}
/*}}}*/

/*{{{ dir_tree_file_write */

typedef struct {
    DirTree_file_write_cb file_write_cb;
    fuse_req_t req;
} FileWriteOpData;

// buffer is written into local file, or error
static void dir_tree_on_buffer_written_cb (FileIO *fop, gpointer ctx, gboolean success, size_t count)
{
    FileWriteOpData *op_data = (FileWriteOpData *) ctx;

    op_data->file_write_cb (op_data->req, success, count);

    LOG_debug (DIR_TREE_LOG, "[fop: %p] buffer written, count: %zu", fop, count);
    
    g_free (op_data);
}

// send data via s3http client
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
        LOG_msg (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", INO ino);
        file_write_cb (req, FALSE,  0);
        return;
    }
    
    fop = en->fop;
    
    LOG_debug (DIR_TREE_LOG, "[fop: %p] write inode %"INO_FMT", size: %zd, off: %"OFF_FMT, fop, INO ino, size, off);

    op_data = g_new0 (FileWriteOpData, 1);
    op_data->file_write_cb = file_write_cb;
    op_data->req = req;

    fileio_write_buffer (fop, buf, size, off, ino, dir_tree_on_buffer_written_cb, op_data);
}
/*}}}*/

/*{{{ dir_tree_file_remove */

typedef struct {
    DirTree *dtree;
    DirEntry *en;
    fuse_ino_t ino;
    DirTree_file_remove_cb file_remove_cb;
    fuse_req_t req;
} FileRemoveData;

// file is removed
static void dir_tree_file_remove_on_con_data_cb (S3HttpConnection *con, gpointer ctx, gboolean success,
        const gchar *buf, size_t buf_len, G_GNUC_UNUSED struct evkeyvalq *headers)
{
    FileRemoveData *data = (FileRemoveData *) ctx;
    
    data->en->removed = TRUE;
    dir_tree_entry_modified (data->dtree, data->en);

    if (data->file_remove_cb)
        data->file_remove_cb (data->req, success);

    
    s3http_connection_release (con);

    g_free (data);
}

// s3http client is ready for a new request
static void dir_tree_file_remove_on_con_cb (gpointer client, gpointer ctx)
{
    S3HttpConnection *con = (S3HttpConnection *) client;
    FileRemoveData *data = (FileRemoveData *) ctx;
    gchar *req_path;
    gboolean res;

    s3http_connection_acquire (con);

    req_path = g_strdup_printf ("/%s", data->en->fullpath);
    res = s3http_connection_make_request (con, 
        req_path, "DELETE", 
        NULL,
        dir_tree_file_remove_on_con_data_cb,
        data
    );
    g_free (req_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create s3http request !");
        data->file_remove_cb (data->req, FALSE);
        
        s3http_connection_release (con);
        g_free (data);
    }
}

// remove file
void dir_tree_file_remove (DirTree *dtree, fuse_ino_t ino, DirTree_file_remove_cb file_remove_cb, fuse_req_t req)
{
    DirEntry *en;
    FileRemoveData *data;
    
    LOG_debug (DIR_TREE_LOG, "Removing  inode %"INO_FMT, INO ino);

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
    data->en = en;
    data->file_remove_cb = file_remove_cb;
    data->req = req;

    s3client_pool_get_client (application_get_ops_client_pool (dtree->app),
        dir_tree_file_remove_on_con_cb, data);
}

void dir_tree_file_unlink (DirTree *dtree, fuse_ino_t parent_ino, const char *name, DirTree_file_remove_cb file_remove_cb, fuse_req_t req)
{
    DirEntry *en, *parent_en;
    FileRemoveData *data;
    
    LOG_debug (DIR_TREE_LOG, "Unlinking %s", name);

    parent_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    if (!parent_en) {
        LOG_err (DIR_TREE_LOG, "Parent not found: %"INO_FMT, parent_ino);
        file_remove_cb (req, FALSE);
        return;
    }

    en = g_hash_table_lookup (parent_en->h_dir_tree, name);
    if (!en) {
        LOG_err (DIR_TREE_LOG, "Parent not found: %"INO_FMT, parent_ino);
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
    DirEntry *en;
    DirTree_dir_remove_cb dir_remove_cb;
    fuse_req_t req;
    GQueue *q_objects_to_remove;
} DirRemoveData;

static void dir_tree_dir_remove_try_to_remove_object (S3HttpConnection *con, DirRemoveData *data);

// object is removed, call remove function again
static void dir_tree_dir_remove_on_object_removed_cb (S3HttpConnection *con, gpointer ctx, gboolean success,
        const gchar *buf, size_t buf_len, G_GNUC_UNUSED struct evkeyvalq *headers)
{
    DirRemoveData *data = (DirRemoveData *) ctx;

    dir_tree_dir_remove_try_to_remove_object (con, data);
}

// check if there is any object left in the queue and remove it
static void dir_tree_dir_remove_try_to_remove_object (S3HttpConnection *con, DirRemoveData *data)
{
    gchar *line;
    gchar *req_path;
    gboolean res;

    // check if all objects are removed
    if (g_queue_is_empty (data->q_objects_to_remove)) {
        LOG_debug (DIR_TREE_LOG, "All objects are removed !");
        s3http_connection_release (con);
        g_queue_free_full (data->q_objects_to_remove, g_free);
        if (data->dir_remove_cb)
            data->dir_remove_cb (data->req, TRUE);
        g_free (data);
        return;
    }

    line = g_queue_pop_tail (data->q_objects_to_remove);

    req_path = g_strdup_printf ("/%s", line);
    g_free (line);

    res = s3http_connection_make_request (con, 
        req_path, "DELETE", 
        NULL,
        dir_tree_dir_remove_on_object_removed_cb,
        data
    );

    g_free (req_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create s3http request !");
        s3http_connection_release (con);
        g_queue_free_full (data->q_objects_to_remove, g_free);
        if (data->dir_remove_cb)
            data->dir_remove_cb (data->req, FALSE);
        g_free (data);
    }

}

// got the list of all objects in the directory
// create list to-remove
static void dir_tree_dir_remove_on_con_objects_cb (S3HttpConnection *con, gpointer ctx, gboolean success,
        const gchar *buf, size_t buf_len, G_GNUC_UNUSED struct evkeyvalq *headers)
{
    DirRemoveData *data = (DirRemoveData *) ctx;
    struct evbuffer *evb;
    char *line;

    if (!success) {
        LOG_err (DIR_TREE_LOG, "Failed to get directory's content !");
        s3http_connection_release (con);
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

// s3http Connection is ready, get list of object in directory
static void dir_tree_dir_remove_on_con_cb (gpointer client, gpointer ctx)
{
    S3HttpConnection *con = (S3HttpConnection *) client;
    DirRemoveData *data = (DirRemoveData *) ctx;
    gchar *req_path;
    gboolean res;

    s3http_connection_acquire (con);

    // XXX: max keys
    req_path = g_strdup_printf ("?prefix=%s/", data->en->fullpath);

    res = s3http_connection_make_request (con, 
        req_path, "GET", 
        NULL,
        dir_tree_dir_remove_on_con_objects_cb,
        data
    );

    g_free (req_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create s3http request !");
        if (data->dir_remove_cb)
            data->dir_remove_cb (data->req, FALSE);
        
        s3http_connection_release (con);
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

    LOG_debug (DIR_TREE_LOG, "Removing dir: %s parent: %"INO_FMT, name, parent_ino);

    parent_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    if (!parent_en || parent_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", parent_ino);
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
    
    // ok, directory is found, get S3HttpConnection
    data = g_new0 (DirRemoveData, 1);
    data->dtree = dtree;
    data->dir_remove_cb = dir_remove_cb;
    data->req = req;
    data->ino = en->ino;
    data->en = en;

    s3client_pool_get_client (application_get_ops_client_pool (dtree->app),
        dir_tree_dir_remove_on_con_cb, data);
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
        LOG_err (DIR_TREE_LOG, "Directory (%"INO_FMT") not found !", parent_ino);
        mkdir_cb (req, FALSE, 0, 0, 0, 0);
        return;
    }
    
    // create a new entry
    en = dir_tree_add_entry (dtree, name, mode, DET_dir, parent_ino, 10, time (NULL));
    if (!en) {
        LOG_err (DIR_TREE_LOG, "Failed to create dir: %s !", name);
        mkdir_cb (req, FALSE, 0, 0, 0, 0);
        return;
    }

    //XXX: set as new 
    en->is_modified = FALSE;
    // do not delete it
    en->age = G_MAXUINT32;
    en->mode = DIR_DEFAULT_MODE;

    mkdir_cb (req, TRUE, en->ino, en->mode, en->size, en->ctime);
}/*}}}*/
