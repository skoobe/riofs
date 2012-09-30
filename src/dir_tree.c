#include "include/dir_tree.h"
#include "include/s3fuse.h"
#include "include/s3http_connection.h"
#include "include/s3http_client.h"
#include "include/s3client_pool.h"

typedef struct {
    fuse_ino_t ino;
    fuse_ino_t parent_ino;
    gchar *basename;
    gchar *fullpath;
    guint64 age;
    
    // type of directory entry
    DirEntryType type;

    off_t size;
    mode_t mode;
    time_t ctime;

    // for type == DET_dir
    char *dir_cache; // FUSE directory cache
    size_t dir_cache_size; // directory cache size
    time_t dir_cache_created;

    GHashTable *h_dir_tree; // name -> data
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

DirTree *dir_tree_create (Application *app)
{
    DirTree *dtree;

    dtree = g_new0 (DirTree, 1);
    dtree->app = app;
    dtree->h_inodes = g_hash_table_new (g_direct_hash, g_direct_equal);
    dtree->max_ino = FUSE_ROOT_ID;
    dtree->current_age = 0;
    dtree->dir_cache_max_time = 5; //XXX
    dtree->current_write_ops = 0;

    dtree->root = dir_tree_add_entry (dtree, "/", DIR_DEFAULT_MODE, DET_dir, 0, 0, time (NULL));

    LOG_debug (DIR_TREE_LOG, "DirTree created");

    return dtree;
}

void dir_tree_destroy (DirTree *dtree)
{
    g_free (dtree);
}

/*{{{ dir_entry operations */
static void dir_entry_destroy (gpointer data)
{
    DirEntry *en = (DirEntry *) data;
    // recursively delete entries
    if (en->h_dir_tree)
        g_hash_table_destroy (en->h_dir_tree);
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

    if (en->age < dtree->current_age) {
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
        LOG_msg (DIR_TREE_LOG, "Directory (ino = %d) not found !", ino);
        readdir_cb (req, FALSE, size, off, NULL, 0);
        return;
    }
    
    t = time (NULL);

    // already have directory buffer in the cache
    if (en->dir_cache_size && t >= en->dir_cache_created && t - en->dir_cache_created <= dtree->dir_cache_max_time) {
        LOG_debug (DIR_TREE_LOG, "Sending directory buffer (ino = %d) from cache !", ino);
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

    //send HTTP request
    s3http_connection_get_directory_listing (dtree->http_con, en->fullpath, ino,
        dir_tree_fill_on_dir_buf_cb, dir_fill_data);
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
        LOG_msg (DIR_TREE_LOG, "Entry '%s' not found !", name);
        lookup_cb (req, FALSE, 0, 0, 0, 0);
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
    DirTree_file_write_cb file_write_cb;
    fuse_req_t c_req;

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

    op_data = g_new0 (DirTreeFileOpData, 1);
    op_data->dtree = dtree;
    op_data->ino = en->ino;
    op_data->op_in_progress = FALSE;
    op_data->q_ranges_requested = g_queue_new ();
    op_data->total_read = 0;
    op_data->tmp_write_fd = 0;
    op_data->http = NULL;

    fi->fh = (uint64_t) op_data;

    // create cb
    file_create_cb (req, TRUE, en->ino, en->mode, en->size, fi);
}
/*}}}*/

static void dir_tree_file_op_on_S3HttpClient_cb (S3HttpClient *http, gpointer pool_ctx);
// existing file is opened, create context data
gboolean dir_tree_file_open (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi)
{
    DirTreeFileOpData *op_data;

    LOG_debug (DIR_TREE_LOG, "dir_tree_open  inode %"INO_FMT, ino);
    
    op_data = g_new0 (DirTreeFileOpData, 1);
    op_data->dtree = dtree;
    op_data->ino = ino;
    op_data->op_in_progress = FALSE;
    op_data->q_ranges_requested = g_queue_new ();
    op_data->total_read = 0;
    op_data->tmp_write_fd = 0;
    op_data->http = NULL;

    fi->fh = (uint64_t) op_data;

    // get S3HttpClient from the pool
    if (!s3http_client_pool_get_S3HttpClient (dtree->http_pool, dir_tree_file_op_on_S3HttpClient_cb, op_data)) {
        LOG_err (DIR_TREE_LOG, "Failed to get S3HTTPClient from the pool !");
        return FALSE;
    }

    return TRUE;
}

// file is closed, free context data
void dir_tree_file_release (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi)
{
    DirTreeFileOpData *op_data = (DirTreeFileOpData *) fi->fh;
    
    LOG_debug (DIR_TREE_LOG, "[%p] dir_tree_file_release  inode %d", op_data->http, ino);
    
    if (op_data->http)
        s3http_client_release (op_data->http);
    
    // releasing written file
    if (op_data->tmp_write_fd) {
        s3http_connection_file_send (dtree->http_con, op_data->tmp_write_fd, op_data->en->fullpath);
        close (op_data->tmp_write_fd);
        //XXX: update dir Tree ?
    }

    g_free (op_data);
}

static void dir_tree_file_read_prepare_request (DirTreeFileOpData *op_data, S3HttpClient *http, off_t off, size_t size);
static void dir_tree_file_read_on_last_chunk_cb (S3HttpClient *http, struct evbuffer *input_buf, gpointer ctx)
{
    gchar *buf;
    size_t buf_len;
    DirTreeFileOpData *op_data = (DirTreeFileOpData *) ctx;

    buf_len = evbuffer_get_length (input_buf);
    buf = (gchar *) evbuffer_pullup (input_buf, buf_len);
    
    op_data->total_read += buf_len;
    LOG_debug (DIR_TREE_LOG, "[%p %p] lTOTAL read: %zu (req: %zu), orig size: %zu, TOTAL: %"OFF_FMT, 
        op_data->c_req, http,
        buf_len, op_data->c_size, op_data->en->size, op_data->total_read);

    op_data->file_read_cb (op_data->c_req, TRUE, buf, buf_len);
    evbuffer_drain (input_buf, buf_len);
    
    // if there are more pending chunk requests 
    if (g_queue_get_length (op_data->q_ranges_requested) > 0) {
        DirTreeFileRange *range;
        range = g_queue_pop_head (op_data->q_ranges_requested);
        op_data->c_size = range->size;
        op_data->c_off = range->off;
        op_data->c_req = range->c_req;

        // perform the next chunk request
        dir_tree_file_read_prepare_request (op_data, http, range->off, range->size);
    } else {
        LOG_debug (DIR_TREE_LOG, "Done downloading !!");
        op_data->op_in_progress = FALSE;
    }
}

// the part of chunk is received
static void dir_tree_file_read_on_chunk_cb (S3HttpClient *http, struct evbuffer *input_buf, gpointer ctx)
{
    gchar *buf;
    size_t buf_len;
    DirTreeFileOpData *op_data = (DirTreeFileOpData *) ctx;

    buf_len = evbuffer_get_length (input_buf);
    buf = (gchar *) evbuffer_pullup (input_buf, buf_len);
    
    // LOG_debug (DIR_TREE_LOG, "Read Object onData callback, in size: %zu", buf_len);
}

// prepare HTTP request
static void dir_tree_file_read_prepare_request (DirTreeFileOpData *op_data, S3HttpClient *http, off_t off, size_t size)
{
    gchar *auth_str;
    char time_str[100];
    time_t t = time (NULL);
    gchar res[1024];
    gchar auth_key[300];
    gchar *url;
    gchar range[300];

    s3http_client_request_reset (http);

    s3http_client_set_cb_ctx (http, op_data);
    s3http_client_set_on_chunk_cb (http, dir_tree_file_read_on_chunk_cb);
    s3http_client_set_on_last_chunk_cb (http, dir_tree_file_read_on_last_chunk_cb);
    s3http_client_set_output_length (http, 0);
    
    strftime (time_str, sizeof (time_str), "%a, %d %b %Y %H:%M:%S GMT", gmtime(&t));
    snprintf (res, sizeof (res), "/%s%s", application_get_bucket_name (op_data->dtree->app), op_data->en->fullpath);

 //   auth_str = s3http_connection_get_auth_string (op_data->dtree->http_con, "GET", "", res);
    snprintf (auth_key, sizeof (auth_key), "AWS %s:%s", application_get_access_key_id (op_data->dtree->app), auth_str);
    snprintf (range, sizeof (range), "bytes=%"OFF_FMT"-%"OFF_FMT, off, off+size - 1);

    s3http_client_add_output_header (http, 
        "Authorization", auth_key);
    s3http_client_add_output_header (http,
        "Date", time_str);
    s3http_client_add_output_header (http,
        "Range", range);

    url = g_strdup_printf ("http://%s%s", application_get_bucket_url (op_data->dtree->app), op_data->en->fullpath);
    
    s3http_client_start_request (http, S3Method_get, url);

    g_free (auth_str);
    g_free (url);
}


// S3HttpClient is ready for the new request execution
// prepare and execute read object call
static void dir_tree_file_op_on_S3HttpClient_cb (S3HttpClient *http, gpointer pool_ctx)
{
    DirTreeFileOpData *op_data = (DirTreeFileOpData *) pool_ctx;
    DirTreeFileRange *range;

    op_data->http = http;
    s3http_client_acquire (op_data->http);
    
    // get the first chunk request
    range = g_queue_pop_head (op_data->q_ranges_requested);
    if (range && op_data->file_read_cb) {
        op_data->c_size = range->size;
        op_data->c_off = range->off;
        op_data->c_req = range->c_req;

    LOG_debug (DIR_TREE_LOG, "[%p %p] S3HTTP client is ready for Read Object inode %"INO_FMT", path: %s", 
        op_data->c_req, http, op_data->ino, op_data->en->fullpath);

        // perform the first request
        dir_tree_file_read_prepare_request (op_data, http, range->off, range->size);
    } else if (range && op_data->file_write_cb) {
    LOG_debug (DIR_TREE_LOG, "[%p] S3HTTP client is ready for Write Object inode %"INO_FMT", path: %s", 
         http, op_data->ino, op_data->en->fullpath);

        ssize_t out_size;
        //XXX: here decide if switch to multi part upload
        out_size = pwrite (op_data->tmp_write_fd, range->write_buf, range->size, range->off);
        if (out_size < 0) {
            op_data->file_write_cb (range->c_req, FALSE, 0);
            return;
        } else
            op_data->file_write_cb (range->c_req, TRUE, out_size);
    }
}

// add new chunk range to the chunks pending queue
void dir_tree_file_read (DirTree *dtree, fuse_ino_t ino, 
    size_t size, off_t off,
    DirTree_file_read_cb file_read_cb, fuse_req_t req,
    struct fuse_file_info *fi)
{
    DirEntry *en;
    char full_name[1024];
    DirTreeFileOpData *op_data = (DirTreeFileOpData *) fi->fh;
    DirTreeFileRange *range;
    
    LOG_debug (DIR_TREE_LOG, "[%p] Read Object  inode %"INO_FMT", size: %zd, off: %"OFF_FMT, req, ino, size, off);

    op_data->file_read_cb = file_read_cb;

    range = g_new0 (DirTreeFileRange, 1);
    range->off = off;
    range->size = size;
    range->c_req = req;
    g_queue_push_tail (op_data->q_ranges_requested, range);

    // already reading data
    if (op_data->op_in_progress) {
        return;
    }
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", ino);
        file_read_cb (req, FALSE, NULL, 0);
        return;
    }

    op_data->en = en;
    op_data->op_in_progress = TRUE;
}

/*{{{ file write */

// send data via HTTP client
void dir_tree_file_write (DirTree *dtree, fuse_ino_t ino, 
    const char *buf, size_t size, off_t off, 
    DirTree_file_write_cb file_write_cb, fuse_req_t req,
    struct fuse_file_info *fi)
{
    DirEntry *en;
    DirTreeFileOpData *op_data = (DirTreeFileOpData *) fi->fh;
    ssize_t out_size;
    
    LOG_debug (DIR_TREE_LOG, "Writing Object  inode %"INO_FMT", size: %zd, off: %"OFF_FMT, ino, size, off);

    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", ino);
        file_write_cb (req, FALSE,  0);
        return;
    }

    op_data->file_write_cb = file_write_cb;

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
    
    //XXX: here decide if switch to multi part upload
    out_size = pwrite (op_data->tmp_write_fd, buf, size, off);
    if (out_size < 0) {
        file_write_cb (req, FALSE, 0);
        return;
    } else
        file_write_cb (req, TRUE, out_size);

}
/*}}}*/

gboolean dir_tree_file_remove (DirTree *dtree, fuse_ino_t ino)
{
    DirEntry *en;
    gchar *req_path;
    gboolean res;
    
    LOG_debug (DIR_TREE_LOG, "Removing  inode %"INO_FMT, ino);

    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") not found !", ino);
        return FALSE;
    }

    if (en->type != DET_file) {
        LOG_err (DIR_TREE_LOG, "Entry (ino = %"INO_FMT") is not a file !", ino);
        return FALSE;
    }

    req_path = g_strdup_printf ("%s", en->fullpath);

    res = s3http_connection_make_request (dtree->http_con, 
        req_path, req_path, "DELETE", 
        NULL,
        NULL,
        NULL,
        NULL
    );

    g_free (req_path);

    if (!res) {
        LOG_err (DIR_TREE_LOG, "Failed to create HTTP request !");
        return FALSE;
    }

    //
    en->age = 0;
    dir_tree_entry_modified (dtree, en);

    return TRUE;
}
