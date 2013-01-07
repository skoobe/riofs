/*
 * Copyright (C) 2012 Paul Ionkin <paul.ionkin@gmail.com>
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
#include "dir_tree.h"
#include "s3fuse.h"
#include "s3http_connection.h"
#include "s3http_client.h"
#include "s3client_pool.h"

typedef struct {
    fuse_ino_t ino;
    fuse_ino_t parent_ino;
    gchar *basename;
    gchar *fullpath;
    guint64 age;
    
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
    gpointer op_data;
} DirEntry;

struct _DirTree {
    DirEntry *root;
    GHashTable *h_inodes; // inode -> DirEntry
    Application *app;

    fuse_ino_t max_ino;
    guint64 current_age;
    time_t dir_cache_max_time; // max time of dir cache in seconds

    gint64 current_write_ops; // the number of current write operations
};

#define DIR_TREE_LOG "dir_tree"
#define DIR_DEFAULT_MODE S_IFDIR | 0755
#define FILE_DEFAULT_MODE S_IFREG | 0444

static DirEntry *dir_tree_add_entry (DirTree *dtree, const gchar *basename, mode_t mode, 
    DirEntryType type, fuse_ino_t parent_ino, off_t size, time_t ctime);
static void dir_tree_entry_modified (DirTree *dtree, DirEntry *en);
static void dir_entry_destroy (gpointer data);

DirTree *dir_tree_create (Application *app)
{
    DirTree *dtree;
    AppConf *conf;

    conf = application_get_conf (app);
    dtree = g_new0 (DirTree, 1);
    dtree->app = app;
    // children entries are destroyed by parent directory entries
    dtree->h_inodes = g_hash_table_new (g_direct_hash, g_direct_equal);
    dtree->max_ino = FUSE_ROOT_ID;
    dtree->current_age = 0;
    dtree->dir_cache_max_time = conf->dir_cache_max_time; //XXX
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
    
    en = g_new0 (DirEntry, 1);

    // get the parent, for inodes > 0
    if (parent_ino) {
        parent_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
        if (!parent_en) {
            LOG_err (DIR_TREE_LOG, "Parent not found for ino: %llu !", parent_ino);
            return NULL;
        }

        // update directory buffer
        dir_tree_entry_modified (dtree, parent_en);

        if (parent_ino == 1)
            en->fullpath = g_strdup_printf ("/%s", basename);
        else
            en->fullpath = g_strdup_printf ("%s/%s", parent_en->fullpath, basename);
    } else {
        en->fullpath = g_strdup ("/");
    }

    en->ino = dtree->max_ino++;
    en->age = dtree->current_age;
    en->basename = g_strdup (basename);
    en->mode = mode;
    en->size = size;
    en->parent_ino = parent_ino;
    en->type = type;
    en->ctime = ctime;
    en->is_modified = FALSE;

    // cache is empty
    en->dir_cache = NULL;
    en->dir_cache_size = 0;
    en->dir_cache_created = 0;

    LOG_debug (DIR_TREE_LOG, "Creating new DirEntry: %s, inode: %d, fullpath: %s, mode: %d", en->basename, en->ino, en->fullpath, en->mode);
    
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
            LOG_debug (DIR_TREE_LOG, "Unsupported: %s", en->fullpath);
            return FALSE;
        } else {
            LOG_debug (DIR_TREE_LOG, "Removing %s", name);
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
        LOG_err (DIR_TREE_LOG, "DirEntry is not a directory ! ino: %"INO_FMT, parent_ino);
        return;
    }
    LOG_debug (DIR_TREE_LOG, "Removing old DirEntries for: %s ..", parent_en->fullpath);

    if (parent_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "Parent is not a directory !");
        return;
    }
    
    g_hash_table_foreach_remove (parent_en->h_dir_tree, dir_tree_stop_update_on_remove_child_cb, dtree);
}

void dir_tree_update_entry (DirTree *dtree, const gchar *path, DirEntryType type, 
    fuse_ino_t parent_ino, const gchar *entry_name, long long size)
{
    DirEntry *parent_en;
    DirEntry *en;

    LOG_debug (DIR_TREE_LOG, "Updating %s %ld", entry_name, size);
    
    // get parent
    parent_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    if (!parent_en || parent_en->type != DET_dir) {
        LOG_err (DIR_TREE_LOG, "DirEntry is not a directory ! ino: %"INO_FMT, parent_ino);
        return;
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
            
        dir_tree_add_entry (dtree, entry_name, mode,
            type, parent_ino, size, time (NULL));
    }
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
            LOG_err (DIR_TREE_LOG, "Parent not found for ino: %"INO_FMT" !", en->ino);
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

// callback: 
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
        s3fuse_add_dirbuf (dir_fill_data->req, &b, ".", dir_fill_data->en->ino);
        s3fuse_add_dirbuf (dir_fill_data->req, &b, "..", dir_fill_data->en->ino);

        LOG_debug (DIR_TREE_LOG, "Entries in directory : %u", g_hash_table_size (dir_fill_data->en->h_dir_tree));
        
        // get all directory items
        g_hash_table_iter_init (&iter, dir_fill_data->en->h_dir_tree);
        while (g_hash_table_iter_next (&iter, NULL, &value)) {
            DirEntry *tmp_en = (DirEntry *) value;
            // add only updated entries
            if (tmp_en->age >= dir_fill_data->dtree->current_age)
                s3fuse_add_dirbuf (dir_fill_data->req, &b, tmp_en->basename, tmp_en->ino);
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

static void dir_tree_fill_dir_on_http_ready (gpointer client, gpointer ctx)
{
    S3HttpConnection *con = (S3HttpConnection *) client;
    DirTreeFillDirData *dir_fill_data = (DirTreeFillDirData *) ctx;

    //send HTTP request
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
    
    LOG_debug (DIR_TREE_LOG, "Requesting directory buffer for dir ino %"INO_FMT", size: %zd, off: %"OFF_FMT, ino, size, off);
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if directory does not exist
    // or it's not a directory type ?
    if (!en || en->type != DET_dir) {
        LOG_msg (DIR_TREE_LOG, "Directory (ino = %"INO_FMT") not found !", ino);
        readdir_cb (req, FALSE, size, off, NULL, 0);
        return;
    }
    
    t = time (NULL);

    // already have directory buffer in the cache
    if (en->dir_cache_size && t >= en->dir_cache_created && t - en->dir_cache_created <= dtree->dir_cache_max_time) {
        LOG_debug (DIR_TREE_LOG, "Sending directory buffer (ino = %"INO_FMT") from cache !", ino);
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

    if (!s3client_pool_get_client (application_get_ops_client_pool (dtree->app), dir_tree_fill_dir_on_http_ready, dir_fill_data)) {
        LOG_err (DIR_TREE_LOG, "Failed to get HTTP client !");
        readdir_cb (req, FALSE, size, off, NULL, 0);
        g_free (dir_fill_data);
    }

}
/*}}}*/

/*{{{ dir_tree_lookup */
// lookup entry and return attributes
void dir_tree_lookup (DirTree *dtree, fuse_ino_t parent_ino, const char *name,
    dir_tree_lookup_cb lookup_cb, fuse_req_t req)
{
    DirEntry *dir_en, *en;
    
    LOG_debug (DIR_TREE_LOG, "Looking up for '%s' in directory ino: %d", name, parent_ino);
    
    dir_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    
    // entry not found
    if (!dir_en || dir_en->type != DET_dir) {
        LOG_msg (DIR_TREE_LOG, "Directory (%d) not found !", parent_ino);
        lookup_cb (req, FALSE, 0, 0, 0, 0);
        return;
    }

    en = g_hash_table_lookup (dir_en->h_dir_tree, name);
    if (!en) {
        LOG_debug (DIR_TREE_LOG, "Entry '%s' not found !", name);
        lookup_cb (req, FALSE, 0, 0, 0, 0);
        return;
    }
    
    // file is removed
    if (en->age == 0) {
        LOG_debug (DIR_TREE_LOG, "Entry '%s' is removed !", name);
        lookup_cb (req, FALSE, 0, 0, 0, 0);
        return;
    }
    
    // hide it
    if (en->is_modified) {
        LOG_debug (DIR_TREE_LOG, "Entry '%s' is modified !", name);
        lookup_cb (req, TRUE, en->ino, en->mode, 0, en->ctime);
        return;
    }

    lookup_cb (req, TRUE, en->ino, en->mode, en->size, en->ctime);
}
/*}}}*/

/*{{{ dir_tree_getattr */
// return entry attributes
void dir_tree_getattr (DirTree *dtree, fuse_ino_t ino, 
    dir_tree_getattr_cb getattr_cb, fuse_req_t req)
{
    DirEntry  *en;
    
    LOG_debug (DIR_TREE_LOG, "Getting attributes for %d", ino);
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));
    
    // entry not found
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Entry (%d) not found !", ino);
        getattr_cb (req, FALSE, 0, 0, 0, 0);
        return;
    }

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
    
    LOG_debug (DIR_TREE_LOG, "Setting attributes for %d", ino);
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));
    
    // entry not found
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Entry (%d) not found !", ino);
        setattr_cb (req, FALSE, 0, 0, 0);
        return;
    }
    //XXX: en->mode
    setattr_cb (req, TRUE, en->ino, en->mode, en->size);
}
/*}}}*/

typedef struct {
    DirTree *dtree;
    DirEntry *en;
    fuse_ino_t ino;
    DirTree_file_read_cb file_read_cb;
    DirTree_file_open_cb file_open_cb;
    fuse_req_t c_req;
    struct fuse_file_info *c_fi;

    size_t c_size;
    off_t c_off;
    S3HttpClient *http;

    int tmp_write_fd; 

    GQueue *q_ranges_requested;
    off_t total_read;
    
    gboolean op_in_progress;
    
} DirTreeFileOpData;

typedef struct {
    size_t size;
    off_t off;
    char *write_buf;
    fuse_req_t c_req;
} DirTreeFileRange;

/*{{{ dir_tree_add_file */

static DirTreeFileOpData *file_op_data_create (DirTree *dtree, fuse_ino_t ino)
{
    DirTreeFileOpData *op_data;
    
    op_data = g_new0 (DirTreeFileOpData, 1);
    op_data->dtree = dtree;
    op_data->ino = ino;
    op_data->op_in_progress = FALSE;
    op_data->q_ranges_requested = g_queue_new ();
    op_data->total_read = 0;
    op_data->tmp_write_fd = 0;
    op_data->http = NULL;

    return op_data;
}

static void file_op_data_destroy (DirTreeFileOpData *op_data)
{
    LOG_debug (DIR_TREE_LOG, "Destroying opdata !");
    
    if (op_data && op_data->q_ranges_requested) {
        if (g_queue_get_length (op_data->q_ranges_requested) > 0)
            g_queue_free_full (op_data->q_ranges_requested, g_free);
        else
            g_queue_free (op_data->q_ranges_requested);
    }
    g_free (op_data);
}

// add new file entry to directory, return new inode
void dir_tree_file_create (DirTree *dtree, fuse_ino_t parent_ino, const char *name, mode_t mode,
    DirTree_file_create_cb file_create_cb, fuse_req_t req, struct fuse_file_info *fi)
{
    DirEntry *dir_en, *en;
    DirTreeFileOpData *op_data;
    
    LOG_debug (DIR_TREE_LOG, "Adding new entry '%s' to directory ino: %"INO_FMT, name, parent_ino);
    
    dir_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    
    // entry not found
    if (!dir_en || dir_en->type != DET_dir) {
        LOG_msg (DIR_TREE_LOG, "Directory (%"INO_FMT") not found !", parent_ino);
        file_create_cb (req, FALSE, 0, 0, 0, fi);
        return;
    }
    
    // create a new entry
    en = dir_tree_add_entry (dtree, name, mode, DET_file, parent_ino, 0, time (NULL));
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Failed to create file: %s !", name);
        file_create_cb (req, FALSE, 0, 0, 0, fi);
        return;
    }
    //XXX: set as new 
    en->is_modified = TRUE;

    op_data = file_op_data_create (dtree, en->ino);
    op_data->en = en;
    op_data->ino = en->ino;
    en->op_data = (gpointer) op_data;
        
    file_create_cb (req, TRUE, en->ino, en->mode, en->size, fi);
}
/*}}}*/

static void dir_tree_file_read_prepare_request (DirTreeFileOpData *op_data, S3HttpClient *http, off_t off, size_t size);
static void dir_tree_file_open_on_http_ready (gpointer client, gpointer ctx);

// existing file is opened, create context data
gboolean dir_tree_file_open (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi, 
    DirTree_file_open_cb file_open_cb, fuse_req_t req)
{
    DirTreeFileOpData *op_data;
    DirEntry *en;

    op_data = file_op_data_create (dtree, ino);
    op_data->c_fi = fi;
    op_data->c_req = req;
    op_data->file_open_cb = file_open_cb;

    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", ino);
        file_open_cb (op_data->c_req, FALSE, op_data->c_fi);
        return FALSE;
    }

    op_data->en = en;
    
    op_data->en->op_data = (gpointer) op_data;

    LOG_debug (DIR_TREE_LOG, "[%p %p] dir_tree_open  inode %"INO_FMT, op_data, fi, ino);

    if (!s3client_pool_get_client (application_get_read_client_pool (dtree->app), dir_tree_file_open_on_http_ready, op_data)) {
        LOG_err (DIR_TREE_LOG, "Failed to get S3HttpConnection from the pool !");
    }

    return TRUE;
}

static void dir_tree_file_release_on_entry_sent_cb (gpointer ctx, gboolean success)
{
    DirTreeFileOpData *op_data = (DirTreeFileOpData *) ctx;
    // XXX: entry may be deleted
    
    op_data->en->is_modified = FALSE;

    close (op_data->tmp_write_fd);

    LOG_debug (DIR_TREE_LOG, "File is sent:  ino = %"INO_FMT")", op_data->ino);
    
    file_op_data_destroy (op_data);
}

// HTTP client is ready for a new request
static void dir_tree_file_release_on_http_ready (gpointer client, gpointer ctx)
{
    S3HttpConnection *http_con = (S3HttpConnection *) client;
    DirTreeFileOpData *op_data = (DirTreeFileOpData *) ctx;

    LOG_debug (DIR_TREE_LOG, "[%p] Acquired http client ! ino: %"INO_FMT, op_data, op_data->ino);

    s3http_connection_acquire (http_con);

    s3http_connection_file_send (http_con, op_data->tmp_write_fd, op_data->en->fullpath, 
        dir_tree_file_release_on_entry_sent_cb, op_data);
}

// file is closed, free context data
void dir_tree_file_release (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi)
{
    DirEntry *en;
    DirTreeFileOpData *op_data;
    
    LOG_debug (DIR_TREE_LOG, "dir_tree_file_release  inode %d", ino);

    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", ino);
        //XXX
        return;
    }

    op_data = (DirTreeFileOpData *) en->op_data;
  //  op_data->en = en;
  //  op_data->ino = ino;
    
    if (op_data->http)
        s3http_client_release (op_data->http);
    
    // releasing written file
    if (op_data->tmp_write_fd) {
        if (!s3client_pool_get_client (application_get_write_client_pool (dtree->app), dir_tree_file_release_on_http_ready, op_data)) {
            LOG_err (DIR_TREE_LOG, "Failed to get S3HttpConnection from the pool !");
        }
    } else {
        file_op_data_destroy (op_data);
    }
}

/*{{{ file read*/
static void dir_tree_file_open_on_http_ready (gpointer client, gpointer ctx)
{
    S3HttpClient *http = (S3HttpClient *) client;
    DirTreeFileOpData *op_data = (DirTreeFileOpData *) ctx;
    DirTreeFileRange *range;

    LOG_debug (DIR_TREE_LOG, "[%p] Acquired http client %s", op_data, op_data->en->fullpath);
    
    s3http_client_acquire (http);
    op_data->http = http;
    
    if (op_data->file_open_cb)
        op_data->file_open_cb (op_data->c_req, TRUE, op_data->c_fi);
    
    op_data->c_req = NULL;
    op_data->c_size = 0;
    op_data->c_req = 0;
    op_data->op_in_progress = FALSE;

    // get the first chunk request
    range = g_queue_pop_head (op_data->q_ranges_requested);
    if (range) {
        op_data->c_size = range->size;
        op_data->c_off = range->off;
        op_data->c_req = range->c_req;
        g_free (range);

        LOG_debug (DIR_TREE_LOG, "[%p %p] S3HTTP client is ready for Read Object inode %"INO_FMT", path: %s", 
            op_data->c_req, http, op_data->ino, op_data->en->fullpath);

        op_data->op_in_progress = TRUE;

        // perform the first request
        dir_tree_file_read_prepare_request (op_data, http, op_data->c_off, op_data->c_size);
    }
}

static void dir_tree_file_read_on_last_chunk_cb (S3HttpClient *http, struct evbuffer *input_buf, gpointer ctx)
{
    gchar *buf = NULL;
    size_t buf_len;
    DirTreeFileOpData *op_data = (DirTreeFileOpData *) ctx;
    DirTreeFileRange *range;

    buf_len = evbuffer_get_length (input_buf);
    buf = (gchar *) evbuffer_pullup (input_buf, buf_len);
    
    /*
    range = g_queue_pop_head (op_data->q_ranges_requested);
    if (range) {
        op_data->c_size = range->size;
        op_data->c_off = range->off;
        op_data->c_req = range->c_req;
    }
    */

    op_data->total_read += buf_len;
    LOG_debug (DIR_TREE_LOG, "[%p %p] lTOTAL read: %zu (req: %zu), orig size: %zu, TOTAL: %"OFF_FMT", Qsize: %zu", 
        op_data->c_req, http,
        buf_len, op_data->c_size, op_data->en->size, op_data->total_read, g_queue_get_length (op_data->q_ranges_requested));
    
    if (op_data->file_read_cb)
        op_data->file_read_cb (op_data->c_req, TRUE, buf, buf_len);

    evbuffer_drain (input_buf, buf_len);
    
    // if there are more pending chunk requests 
    if (g_queue_get_length (op_data->q_ranges_requested) > 0) {
        range = g_queue_pop_head (op_data->q_ranges_requested);
        LOG_debug (DIR_TREE_LOG, "[%p] more data: %zd", range->c_req, range->size);
        op_data->c_size = range->size;
        op_data->c_off = range->off;
        op_data->c_req = range->c_req;
        g_free (range);

        op_data->op_in_progress = TRUE;
        // perform the next chunk request
        dir_tree_file_read_prepare_request (op_data, http, op_data->c_off, op_data->c_size);
    } else {
        LOG_debug (DIR_TREE_LOG, "Done downloading !!");
        op_data->op_in_progress = FALSE;
    }
}

// the part of chunk is received
/* unused for now
static void dir_tree_file_read_on_chunk_cb (S3HttpClient *http, struct evbuffer *input_buf, gpointer ctx)
{
    gchar *buf;
    size_t buf_len;
    DirTreeFileOpData *op_data = (DirTreeFileOpData *) ctx;

    buf_len = evbuffer_get_length (input_buf);
    buf = (gchar *) evbuffer_pullup (input_buf, buf_len);
    
    // LOG_debug (DIR_TREE_LOG, "Read Object onData callback, in size: %zu", buf_len);
}
*/

// prepare HTTP request
static void dir_tree_file_read_prepare_request (DirTreeFileOpData *op_data, S3HttpClient *http, off_t off, size_t size)
{
    gchar *auth_str;
    char time_str[100];
    time_t t = time (NULL);
    gchar auth_key[300];
    gchar *url;
    gchar range[300];
    AppConf *conf;

    s3http_client_request_reset (http);

    s3http_client_set_cb_ctx (http, op_data);
//    s3http_client_set_on_chunk_cb (http, dir_tree_file_read_on_chunk_cb);
    s3http_client_set_on_last_chunk_cb (http, dir_tree_file_read_on_last_chunk_cb);
    s3http_client_set_output_length (http, 0);
    
    strftime (time_str, sizeof (time_str), "%a, %d %b %Y %H:%M:%S GMT", gmtime(&t));

    auth_str = (gchar *)s3http_connection_get_auth_string (op_data->dtree->app, "GET", "", op_data->en->fullpath, time_str);
    snprintf (auth_key, sizeof (auth_key), "AWS %s:%s", application_get_access_key_id (op_data->dtree->app), auth_str);
    g_free (auth_str);
    snprintf (range, sizeof (range), "bytes=%"OFF_FMT"-%"OFF_FMT, off, off+size - 1);
    LOG_debug (DIR_TREE_LOG, "range: %s", range);

    s3http_client_add_output_header (http, "Authorization", auth_key);
    s3http_client_add_output_header (http, "Date", time_str);
    s3http_client_add_output_header (http, "Range", range);
    s3http_client_add_output_header (http, "Host", application_get_host_header (op_data->dtree->app));

    // XXX: HTTPS
    conf = application_get_conf (op_data->dtree->app);
    if (conf->path_style) {
        url = g_strdup_printf ("http://%s:%d/%s%s", application_get_host (op_data->dtree->app),
                                                    application_get_port (op_data->dtree->app),
                                                    application_get_bucket_name (op_data->dtree->app),
                                                    op_data->en->fullpath);
    } else {
        url = g_strdup_printf ("http://%s%d%s", application_get_host (op_data->dtree->app),
                                                application_get_port (op_data->dtree->app),
                                                op_data->en->fullpath);
    }
    
    s3http_client_start_request (http, S3Method_get, url);

    g_free (auth_str);
    g_free (url);
}

// add new chunk range to the chunks pending queue
void dir_tree_file_read (DirTree *dtree, fuse_ino_t ino, 
    size_t size, off_t off,
    DirTree_file_read_cb file_read_cb, fuse_req_t req,
    struct fuse_file_info *fi)
{
    DirEntry *en;
    char full_name[1024];
    DirTreeFileOpData *op_data;
    DirTreeFileRange *range;
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", ino);
        file_read_cb (req, FALSE, NULL, 0);
        return;
    }
    
    op_data = (DirTreeFileOpData *) en->op_data;
    
    LOG_debug (DIR_TREE_LOG, "[%p %p] Read Object  inode %"INO_FMT", size: %zd, off: %"OFF_FMT, req, op_data, ino, size, off);
    
    op_data->file_read_cb = file_read_cb;
    op_data->en = en;
    op_data->dtree = dtree;

    range = g_new0 (DirTreeFileRange, 1);
    range->off = off;
    range->size = size;
    range->c_req = req;
    g_queue_push_tail (op_data->q_ranges_requested, range);
    LOG_debug (DIR_TREE_LOG, "[%p] more data b: %zd", range->c_req, range->size);

    // already reading data
    if (op_data->op_in_progress) {
        return;
    }

    if (op_data->http) {
        LOG_debug (DIR_TREE_LOG, "Adding from main");
        range = g_queue_pop_head (op_data->q_ranges_requested);
        if (range) {
            op_data->c_size = range->size;
            op_data->c_off = range->off;
            op_data->c_req = range->c_req;
            g_free (range);
            
            // perform the next chunk request
            op_data->op_in_progress = TRUE;
            dir_tree_file_read_prepare_request (op_data, op_data->http, op_data->c_off, op_data->c_size);
        }
        
        return;
    }

}
/*}}}*/

/*{{{ file write */

// send data via HTTP client
void dir_tree_file_write (DirTree *dtree, fuse_ino_t ino, 
    const char *buf, size_t size, off_t off, 
    DirTree_file_write_cb file_write_cb, fuse_req_t req,
    struct fuse_file_info *fi)
{
    DirEntry *en;
    DirTreeFileOpData *op_data;
    ssize_t out_size;

    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", ino);
        file_write_cb (req, FALSE,  0);
        return;
    }
    
    op_data = (DirTreeFileOpData *) en->op_data;
    
    LOG_debug (DIR_TREE_LOG, "[%p] Writing Object  inode %"INO_FMT", size: %zd, off: %"OFF_FMT, op_data, ino, size, off);

    // if tmp file is not opened
    if (!op_data->tmp_write_fd) {
        char filename[1024];

        op_data->en = en;
        op_data->op_in_progress = TRUE;
        snprintf (filename, sizeof (filename), "%s/s3ffs.XXXXXX", application_get_tmp_dir (dtree->app));
        op_data->tmp_write_fd = mkstemp (filename);
        if (op_data->tmp_write_fd < 0) {
            LOG_err (DIR_TREE_LOG, "Failed to create tmp file !");
            file_write_cb (req, FALSE, 0);
            return;
        }
    }

    // if http client is not acquired yet
    //if (!op_data->con_http) {
    //}
    
    //XXX: here decide if switch to multi part upload
    out_size = pwrite (op_data->tmp_write_fd, buf, size, off);
    if (out_size < 0) {
        file_write_cb (req, FALSE, 0);
        return;
    } else
        file_write_cb (req, TRUE, out_size);

}
/*}}}*/

/*{{{ file remove*/

typedef struct {
    DirTree *dtree;
    DirEntry *en;
    fuse_ino_t ino;
    DirTree_file_remove_cb file_remove_cb;
    fuse_req_t req;
} FileRemoveData;

// file is removed
static void dir_tree_file_remove_on_http_client_data_cb (S3HttpConnection *http_con, gpointer ctx, 
        const gchar *buf, size_t buf_len, G_GNUC_UNUSED struct evkeyvalq *headers)
{
    FileRemoveData *data = (FileRemoveData *) ctx;
    
    data->en->age = 0;
    dir_tree_entry_modified (data->dtree, data->en);
    data->file_remove_cb (data->req, TRUE);

    g_free (data);
    
    s3http_connection_release (http_con);
}

// error 
static void dir_tree_file_remove_on_http_client_error_cb (S3HttpConnection *http_con, gpointer ctx)
{
    FileRemoveData *data = (FileRemoveData *) ctx;
    
    data->file_remove_cb (data->req, FALSE);

    g_free (data);
    
    s3http_connection_release (http_con);
}

// HTTP client is ready for a new request
static void dir_tree_file_remove_on_http_client_cb (gpointer client, gpointer ctx)
{
    S3HttpConnection *http_con = (S3HttpConnection *) client;
    FileRemoveData *data = (FileRemoveData *) ctx;
    gchar *req_path;
    gboolean res;

    s3http_connection_acquire (http_con);

    req_path = g_strdup_printf ("%s", data->en->fullpath);

    res = s3http_connection_make_request (http_con, 
        req_path, req_path, "DELETE", 
        NULL,
        dir_tree_file_remove_on_http_client_data_cb,
        dir_tree_file_remove_on_http_client_error_cb,
        data
    );

    g_free (req_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create HTTP request !");
        data->file_remove_cb (data->req, FALSE);
        
        s3http_connection_release (http_con);
        g_free (data);
    }
}

// remove file
gboolean dir_tree_file_remove (DirTree *dtree, fuse_ino_t ino, DirTree_file_remove_cb file_remove_cb, fuse_req_t req)
{
    DirEntry *en;
    FileRemoveData *data;
    
    LOG_debug (DIR_TREE_LOG, "Removing  inode %"INO_FMT, ino);

    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", ino);
        file_remove_cb (req, FALSE);
        return FALSE;
    }

    if (en->type != DET_file) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") is not a file !", ino);
        file_remove_cb (req, FALSE);
        return FALSE;
    }

    data = g_new0 (FileRemoveData, 1);
    data->dtree = dtree;
    data->ino = ino;
    data->en = en;
    data->file_remove_cb = file_remove_cb;
    data->req = req;

    s3client_pool_get_client (application_get_ops_client_pool (dtree->app),
        dir_tree_file_remove_on_http_client_cb, data);
        
    return TRUE;
}
/*}}}*/

void dir_tree_dir_create (DirTree *dtree, fuse_ino_t parent_ino, const char *name, mode_t mode,
     dir_tree_mkdir_cb mkdir_cb, fuse_req_t req)
{
    DirEntry *dir_en, *en;
    DirTreeFileOpData *op_data;
    
    LOG_debug (DIR_TREE_LOG, "Creating dir: %s", name);
    
    dir_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    
    // entry not found
    if (!dir_en || dir_en->type != DET_dir) {
        LOG_msg (DIR_TREE_LOG, "Directory (%"INO_FMT") not found !", parent_ino);
        mkdir_cb (req, FALSE, 0, 0, 0, 0);
        return;
    }
    
    // create a new entry
    en = dir_tree_add_entry (dtree, name, mode, DET_dir, parent_ino, 10, time (NULL));
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Failed to create dir: %s !", name);
        mkdir_cb (req, FALSE, 0, 0, 0, 0);
        return;
    }

    //XXX: set as new 
    en->is_modified = FALSE;
    // do not delete it
    en->age = G_MAXUINT32;
    en->mode = DIR_DEFAULT_MODE;

    mkdir_cb (req, TRUE, en->ino, en->mode, en->size, en->ctime);
}
