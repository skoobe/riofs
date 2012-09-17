#include "include/dir_tree.h"
#include "include/fuse.h"
#include "include/bucket_connection.h"
#include "include/s3http_client.h"

typedef enum {
    DET_dir = 0,
    DET_file = 1,
} DirEntryType;

typedef struct {
    fuse_ino_t ino;
    fuse_ino_t parent_ino;
    gchar *name;
    guint64 age;
    
    // type of directory entry
    DirEntryType type;

    // for type == DET_file
    guint64 size;
    mode_t mode;

    // for type == DET_dir
    char *dir_cache; // FUSE directory cache
    size_t dir_cache_size; // directory cache size
    GHashTable *h_dir_tree; // name -> data
} DirEntry;

struct _DirTree {
    DirEntry *root;
    GHashTable *h_inodes; // inode -> DirEntry
    Application *app;

    fuse_ino_t max_ino;
    guint64 current_age;
};

static DirEntry *dir_tree_create_directory (DirTree *dtree, const gchar *name, mode_t mode);


DirTree *dir_tree_create (Application *app)
{
    DirTree *dtree;

    dtree = g_new0 (DirTree, 1);
    dtree->app = app;
    dtree->h_inodes = g_hash_table_new (g_direct_hash, g_direct_equal);
    dtree->max_ino = 1;
    dtree->current_age = 0;

    dtree->root = dir_tree_create_directory (dtree, "/", S_IFDIR | 0755);

    return dtree;
}

void dir_tree_destroy (DirTree *dtree)
{
    g_free (dtree);
}

static void dir_entry_destroy (gpointer data)
{
    DirEntry *en = (DirEntry *) data;
    // recursively delete entries
    g_hash_table_destroy (en->h_dir_tree);
    g_free (en->name);
    g_free (en);
}

static DirEntry *dir_tree_create_directory (DirTree *dtree, const gchar *name, mode_t mode)
{
    DirEntry *en;

    en = g_new0 (DirEntry, 1);
    en->ino = dtree->max_ino++;
    en->age = dtree->current_age;
    en->name = g_strdup (name);
    en->mode = mode;
    
    en->type = DET_dir;
    en->dir_cache = NULL;
    en->dir_cache_size = 0;
    en->h_dir_tree = g_hash_table_new_full (g_str_hash, g_str_equal, NULL, dir_entry_destroy);

    g_hash_table_insert (dtree->h_inodes, GUINT_TO_POINTER (en->ino), en);

    return en;
}

static void dir_tree_directory_add_file (DirEntry *dir, DirEntry *file)
{
    g_hash_table_insert (dir->h_dir_tree, file->name, file);
}

static DirEntry *dir_tree_create_file (DirTree *dtree, const gchar *name, guint64 size, mode_t mode)
{
    DirEntry *en;

    en = g_new0 (DirEntry, 1);
    en->ino = dtree->max_ino++;
    en->age = dtree->current_age;
    en->name = g_strdup (name);
    en->type = DET_file;
    en->size = size;
    en->mode = mode;

    LOG_debug ("Creating new file '%s' inode: %d", en->name, en->ino);

    g_hash_table_insert (dtree->h_inodes, GUINT_TO_POINTER (en->ino), en);

    return en;
}

DirEntry *dir_tree_get_entry_by_path (DirTree *dtree, const gchar *path)
{
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

void dir_tree_update_entry (DirTree *dtree, const gchar *path, const gchar *entry_name, long long size)
{
    DirEntry *root_en;
    DirEntry *en;

    LOG_debug ("Updating %s %ld", entry_name, size);
    
    root_en = dtree->root;
    
    en = g_hash_table_lookup (root_en->h_dir_tree, entry_name);
    if (en) {
        en->age = dtree->current_age;
    } else {
        //XXX: default mode
        en = dir_tree_create_file (dtree, entry_name, size, S_IFREG | 0444);
        dir_tree_directory_add_file (root_en, en);
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

/*{{{ dir_tree_fill_dir_buf */

// return directory buffer from the cache
// or regenerate directory cache
void dir_tree_fill_dir_buf (DirTree *dtree, 
        fuse_ino_t ino, size_t size, off_t off,
        dir_tree_readdir_cb readdir_cb, fuse_req_t req)
{
    DirEntry *en;
    GHashTableIter iter;
    struct dirbuf b; // directory buffer
    gpointer value;
    
    LOG_debug ("Requesting directory buffer for dir ino %d, size: %zd, off: %d", ino, size, off);
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if directory does not exist
    // or it's not a directory type ?
    if (!en || en->type != DET_dir) {
        LOG_msg ("Directory (ino = %d) not found !", ino);
        readdir_cb (req, FALSE, size, off, NULL, 0);
        return;
    }
    
    // already have directory buffer in the cache
    if (en->dir_cache_size) {
        LOG_debug ("Sending directory buffer (ino = %d) from cache !", ino);
        readdir_cb (req, TRUE, size, off, en->dir_cache, en->dir_cache_size);
        return;
    }
    
    //XXX: send HTTP request

    // construct directory buffer
    // add "." and ".."
    memset (&b, 0, sizeof(b));
    s3fuse_add_dirbuf (req, &b, ".", en->ino);
    s3fuse_add_dirbuf (req, &b, "..", en->ino);
    
    // get all directory items
    g_hash_table_iter_init (&iter, en->h_dir_tree);
    while (g_hash_table_iter_next (&iter, NULL, &value)) {
        DirEntry *tmp_en = (DirEntry *) value;
        s3fuse_add_dirbuf (req, &b, tmp_en->name, tmp_en->ino);
    }
    // done, save as cache
    en->dir_cache_size = b.size;
    en->dir_cache = g_malloc (b.size);
    memcpy (en->dir_cache, b.p, b.size);
    // send buffer to fuse
    readdir_cb (req, TRUE, size, off, b.p, b.size);

    //free buffer
    g_free (b.p);
}
/*}}}*/

/*{{{ dir_tree_lookup */
// lookup entry and return attributes
void dir_tree_lookup (DirTree *dtree, fuse_ino_t parent_ino, const char *name,
    dir_tree_lookup_cb lookup_cb, fuse_req_t req)
{
    DirEntry *dir_en, *en;
    
    LOG_debug ("Looking up for '%s' in directory ino: %d", name, parent_ino);
    
    dir_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    
    // entry not found
    if (!dir_en || dir_en->type != DET_dir) {
        LOG_msg ("Directory (%d) not found !", parent_ino);
        lookup_cb (req, FALSE, 0, 0, 0);
        return;
    }

    en = g_hash_table_lookup (dir_en->h_dir_tree, name);
    if (!en) {
        LOG_msg ("Entry '%s' not found !", name);
        lookup_cb (req, FALSE, 0, 0, 0);
        return;
    }

    lookup_cb (req, TRUE, en->ino, en->mode, en->size);
}
/*}}}*/

/*{{{ dir_tree_getattr */
// return entry attributes
void dir_tree_getattr (DirTree *dtree, fuse_ino_t ino, 
    dir_tree_getattr_cb getattr_cb, fuse_req_t req)
{
    DirEntry  *en;
    
    LOG_debug ("Getting attributes for %d", ino);
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));
    
    // entry not found
    if (!en) {
        LOG_msg ("Entry (%d) not found !", ino);
        getattr_cb (req, FALSE, 0, 0, 0);
        return;
    }

    getattr_cb (req, TRUE, en->ino, en->mode, en->size);
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
    
    LOG_debug ("Setting attributes for %d", ino);
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));
    
    // entry not found
    if (!en) {
        LOG_msg ("Entry (%d) not found !", ino);
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

    LOG_debug ("Read object callback  success: %s", success?"YES":"NO");

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
    BucketConnection *con;
    char full_name[1024];
    DirTreeReadData *data;

    
    LOG_debug ("Read Object  inode %d, size: %zd, off: %d", ino, size, off);
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_msg ("Entry (ino = %d) not found !", ino);
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
    S3Connection *con;
    S3Bucket *bucket;
    
    LOG_debug ("Adding new entry '%s' to directory ino: %d", name, parent_ino);
    
    dir_en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (parent_ino));
    
    // entry not found
    if (!dir_en || dir_en->type != DET_dir) {
        LOG_msg ("Directory (%d) not found !", parent_ino);
        create_file_cb (req, FALSE, 0, 0, 0, fi);
        return;
    }
    
    // create a new entry
    en = dir_tree_create_file (dtree, name, 0, mode);
    // add to parent directory
    g_hash_table_insert (dir_en->h_dir_tree, en->name, en);
    // update directory buffer
    dir_tree_entry_modified (dtree, dir_en);
    
    //XXX: from open

    // execute callback
    create_file_cb (req, TRUE, en->ino, mode, en->size, fi);

    bucket = application_get_s3bucket (dtree->app);
    con = s3http_new (application_get_evbase (dtree->app), application_get_dnsbase (dtree->app), 
        S3Method_put, bucket->s_uri
    );
    fi->fh = (uint64_t) con;
}
/*}}}*/

void dir_tree_open (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi)
{

    LOG_debug ("dir_tree_open  inode %d", ino);

    
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

    LOG_debug ("Buffer sent !");
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
    
    LOG_debug ("Writing Object  inode %d, size: %zd, off: %d", ino, size, off);

    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_msg ("Entry (ino = %d) not found !", ino);
        write_cb (req, FALSE,  0);
        return;
    }
    
    // fi->fh contains output buffer
  
    write_cb (req, TRUE,  size);
}



void dir_tree_release (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi)
{
    LOG_debug ("dir_tree_release  inode %d", ino);
}
