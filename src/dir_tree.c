#include "include/dir_tree.h"

typedef struct {
    char *dir_cache;
    size_t dir_cache_size;
    gchar *name;
    guint64 age;
    GHashTable *h_dir_tree; // name -> data
} DirEntry;

struct _DirTree {
    DirEntry *root;
    GHashTable *h_inodes; // inode -> DirEntry
    Application *app;
};

DirTree *dir_tree_create (Application *app)
{
    DirTree *dtree;

    dtree = g_new0 (DirTree, 1);
    dtree->app = app;
    dtree->root = NULL;
    dtree->h_inodes = g_hash_table_new (g_direct_hash, g_direct_equal);

    return dtree;
}

void dir_tree_destroy (DirTree *dtree)
{
    g_free (dtree);
}

DirEntry *dir_tree_get_entry_by_path (DirTree *dtree, const gchar *path)
{
}

// increase the age of directory
void dir_tree_start_update (DirTree *dtree, const gchar *dir_path)
{
}

// remove all entries which age is less than current
void dir_tree_stop_update (DirTree *dtree, const gchar *dir_path)
{
}

void dir_tree_update_entry (DirTree *dtree, const gchar *path, const gchar *entry_name, long long size)
{
    LOG_debug ("Updating %s %ld", entry_name, size);
}


gboolean dir_tree_fill_dir_buf (DirTree *dtree, 
        fuse_ino_t ino, size_t size, off_t off,
        dir_tree_readdir_cb, gpointer readdir_cb_data)
{

    return TRUE;
}
