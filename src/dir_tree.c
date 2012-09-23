#include "include/dir_tree.h"
#include "include/s3fuse.h"
#include "include/s3http_connection.h"
#include "include/s3http_client.h"


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
    GHashTable *h_dir_tree; // name -> data
} DirEntry;

struct _DirTree {
    DirEntry *root;
    GHashTable *h_inodes; // inode -> DirEntry
    Application *app;

    S3HttpConnection *http_con;

    fuse_ino_t max_ino;
    guint64 current_age;
};

#define DIR_TREE_LOG "dir_tree"
#define DIR_DEFAULT_MODE S_IFDIR | 0755
#define FILE_DEFAULT_MODE S_IFREG | 0444

static DirEntry *dir_tree_add_entry (DirTree *dtree, const gchar *basename, mode_t mode, 
    DirEntryType type, fuse_ino_t parent_ino, off_t size, time_t ctime);

DirTree *dir_tree_create (Application *app)
{
    DirTree *dtree;

    dtree = g_new0 (DirTree, 1);
    dtree->app = app;
    dtree->h_inodes = g_hash_table_new (g_direct_hash, g_direct_equal);
    dtree->max_ino = 1;
    dtree->current_age = 0;
    dtree->http_con = application_get_s3http_connection (app);

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
    dtree->current_age++;
}

// remove all entries which age is less than current
void dir_tree_stop_update (DirTree *dtree, const gchar *dir_path)
{
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
void dir_tree_entry_modified (DirTree *dtree, DirEntry *en)
{
    if (en->type == DET_dir) {
        if (en->dir_cache_size) {
            g_free (en->dir_cache);
            en->dir_cache_size = 0;
        }
    } else {
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
            s3fuse_add_dirbuf (dir_fill_data->req, &b, tmp_en->basename, tmp_en->ino);
        }
        // done, save as cache
        dir_fill_data->en->dir_cache_size = b.size;
        dir_fill_data->en->dir_cache = g_malloc (b.size);
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
    
    LOG_debug (DIR_TREE_LOG, "Requesting directory buffer for dir ino %"INO_FMT", size: %zd, off: %"OFF_FMT, ino, size, off);
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if directory does not exist
    // or it's not a directory type ?
    if (!en || en->type != DET_dir) {
        LOG_msg (DIR_TREE_LOG, "Directory (ino = %d) not found !", ino);
        readdir_cb (req, FALSE, size, off, NULL, 0);
        return;
    }
    
    // already have directory buffer in the cache
    if (en->dir_cache_size) {
        LOG_debug (DIR_TREE_LOG, "Sending directory buffer (ino = %d) from cache !", ino);
        readdir_cb (req, TRUE, size, off, en->dir_cache, en->dir_cache_size);
        return;
    }

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

/*{{{ dir_tree_read*/

typedef struct {
    dir_tree_read_cb read_cb;
    fuse_req_t req;
    size_t size;
    off_t off;
} DirTreeReadData;

static void dir_tree_read_callback (gpointer callback_data, gboolean success, struct evbuffer *in_data)
{
    DirTreeReadData *data = (DirTreeReadData *) callback_data;
    char *buf;
    size_t buf_len;

    LOG_debug (DIR_TREE_LOG, "Read object callback  success: %s", success?"YES":"NO");

    if (!success) {
        data->read_cb (data->req, FALSE, data->size, data->off, NULL, 0);
    } else {
        // copy buffer
  //      evbuffer_add_buffer_reference (data->fop->buf, in_data);

        buf_len = evbuffer_get_length (in_data);
        buf = evbuffer_pullup (in_data, buf_len);
        data->read_cb (data->req, TRUE, data->size, data->off, buf, buf_len);
    }

    g_free (data);
}

// return entry's buffer
void dir_tree_read (DirTree *dtree, fuse_ino_t ino, 
    size_t size, off_t off,
    dir_tree_read_cb read_cb, fuse_req_t req,
    struct fuse_file_info *fi)
{
    DirEntry *en;
    S3HttpConnection *con;
    char full_name[1024];
    DirTreeReadData *data;

    
    LOG_debug (DIR_TREE_LOG, "Read Object  inode %d, size: %zd, off: %d", ino, size, off);
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Entry (ino = %d) not found !", ino);
        read_cb (req, FALSE, size, off, NULL, 0);
        return;
    }

}/*}}}*/

/*{{{ dir_tree_add_file */
// add new file entry to directory, return new inode
void dir_tree_add_file (DirTree *dtree, fuse_ino_t parent_ino, const char *name, mode_t mode,
    dir_tree_create_file_cb create_file_cb, fuse_req_t req, struct fuse_file_info *fi)
{
    DirEntry *dir_en, *en;
    S3HttpConnection *con;
    
    LOG_debug (DIR_TREE_LOG, "Adding new entry '%s' to directory ino: %d", name, parent_ino);
    
    dir_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    
    // entry not found
    if (!dir_en || dir_en->type != DET_dir) {
        LOG_msg (DIR_TREE_LOG, "Directory (%d) not found !", parent_ino);
        create_file_cb (req, FALSE, 0, 0, 0, fi);
        return;
    }
    
    // create a new entry
   // en = dir_tree_create_file (dtree, name, 0, mode);
    // add to parent directory
    g_hash_table_insert (dir_en->h_dir_tree, en->basename, en);
    // update directory buffer
    dir_tree_entry_modified (dtree, dir_en);
    
    //XXX: from open
/*
    // execute callback
    create_file_cb (req, TRUE, en->ino, mode, en->size, fi);

    bucket = application_get_s3bucket (dtree->app);
    con = s3http_new (application_get_evbase (dtree->app), application_get_dnsbase (dtree->app), 
        S3Method_put, bucket->s_uri
    );
    fi->fh = (uint64_t) con;
    */
}
/*}}}*/

void dir_tree_open (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi)
{

    LOG_debug (DIR_TREE_LOG, "dir_tree_open  inode %d", ino);

    
}

typedef struct {
    dir_tree_write_cb write_cb;
    fuse_req_t req;
    size_t size;
    off_t off;
} DirTreeWriteData;


// buffer is sent
static void dir_tree_write_on_data_sent (gpointer ctx)
{
    DirTreeWriteData *data = (DirTreeWriteData *) ctx;

    LOG_debug (DIR_TREE_LOG, "Buffer sent !");
    data->write_cb (data->req, TRUE, data->size);
    g_free (data);
}

// write data to output buf
// data will be sent in flush () function
// XXX: add caching
void dir_tree_write (DirTree *dtree, fuse_ino_t ino, 
    const char *buf, size_t size, off_t off, 
    dir_tree_write_cb write_cb, fuse_req_t req,
    struct fuse_file_info *fi)
{
    DirEntry *en;
    size_t out_buf_len;
    DirTreeWriteData *data;
    
    LOG_debug (DIR_TREE_LOG, "Writing Object  inode %d, size: %zd, off: %d", ino, size, off);

    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_msg (DIR_TREE_LOG, "Entry (ino = %d) not found !", ino);
        write_cb (req, FALSE,  0);
        return;
    }
    
    // fi->fh contains output buffer
  
    write_cb (req, TRUE,  size);
}



void dir_tree_release (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi)
{
    LOG_debug (DIR_TREE_LOG, "dir_tree_release  inode %d", ino);
}
