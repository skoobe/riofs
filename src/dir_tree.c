#include "include/dir_tree.h"

typedef enum {
    DET_dir = 0,
    DET_file = 1,
} DirEntryType;

typedef struct {
    fuse_ino_t ino;
    gchar *name;
    guint64 age;

    DirEntryType type;

    // for type == DET_file
    guint64 size;

    // for type == DET_dir
    char *dir_cache;
    size_t dir_cache_size;
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

static dir_tree_directory_add_file (DirEntry *dir, DirEntry *file)
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


gboolean dir_tree_fill_dir_buf (DirTree *dtree, 
        fuse_ino_t ino, size_t size, off_t off,
        dir_tree_readdir_cb readdir_cb, gpointer readdir_cb_data)
{
    DirEntry *en;
    
    en = g_hash_table_lookup (dtree->h_inodes, GUINT_TO_POINTER (ino));

    LOG_debug ("Requesting directory cache for dir ino = %d", ino);

    if (!en || en->type != DET_dir) {
        LOG_err ("Directory (ino = %d) not found !", ino);
        return FALSE;
    }

    // regenerate cache
    if (!en->dir_cache_size) {
    }

    return TRUE;
}
