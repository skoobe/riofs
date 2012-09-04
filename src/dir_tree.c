#include "include/dir_tree.h"
#include "include/fuse.h"
#include "include/bucket_connection.h"

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

static DirEntry *dir_tree_create_directory (DirTree *dtree, const gchar *name);


DirTree *dir_tree_create (Application *app)
{
    DirTree *dtree;

    dtree = g_new0 (DirTree, 1);
    dtree->app = app;
    dtree->h_inodes = g_hash_table_new (g_direct_hash, g_direct_equal);
    dtree->max_ino = 1;
    dtree->current_age = 0;

    dtree->root = dir_tree_create_directory (dtree, "/");

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

static DirEntry *dir_tree_create_directory (DirTree *dtree, const gchar *name)
{
    DirEntry *en;

    en = g_new0 (DirEntry, 1);
    en->ino = dtree->max_ino++;
    en->age = dtree->current_age;
    en->name = g_strdup (name);
    
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

static DirEntry *dir_tree_create_file (DirTree *dtree, const gchar *name, guint64 size)
{
    DirEntry *en;

    en = g_new0 (DirEntry, 1);
    en->ino = dtree->max_ino++;
    en->age = dtree->current_age;
    en->name = g_strdup (name);
    en->type = DET_file;
    en->size = size;

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
        en = dir_tree_create_file (dtree, entry_name, size);
        dir_tree_directory_add_file (root_en, en);
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
    int mode;
    
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

    if (en->type == DET_file) {
        mode = S_IFREG | 0444;
    } else {
        mode = S_IFDIR | 0755;
    }
    lookup_cb (req, TRUE, en->ino, mode, en->size);
}
/*}}}*/

/*{{{ dir_tree_getattr */
// return entry attributes
void dir_tree_getattr (DirTree *dtree, fuse_ino_t ino, 
    dir_tree_getattr_cb getattr_cb, fuse_req_t req)
{
    DirEntry  *en;
    int mode;
    
    LOG_debug ("Getting attributes for %d", ino);
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));
    
    // entry not found
    if (!en) {
        LOG_msg ("Entry (%d) not found !", ino);
        getattr_cb (req, FALSE, 0, 0, 0);
        return;
    }

    if (en->type == DET_file) {
        mode = S_IFREG | 0444;
    } else {
        mode = S_IFDIR | 0755;
    }
    getattr_cb (req, TRUE, en->ino, mode, en->size);
}
/*}}}*/

/*{{{ */
// return entry's buffer
void dir_tree_read (DirTree *dtree, fuse_ino_t ino, 
    size_t size, off_t off,
    dir_tree_read_cb read_cb, fuse_req_t req)
{
    DirEntry *en;
    BucketConnection *con;
    char full_name[1024];
    
    LOG_debug ("Reading buffer  inode %d, size: %zd, off: %d", ino, size, off);
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    // if entry does not exist
    // or it's not a directory type ?
    if (!en) {
        LOG_msg ("Entry (ino = %d) not found !", ino);
        read_cb (req, FALSE, size, off, NULL, 0);
        return;
    }
    con = application_get_con (dtree->app);

    snprintf (full_name, sizeof (full_name), "/%s", en->name);
    // XXX: caching alg
    bucket_connection_get_object (con, full_name);

}/*}}}*/
