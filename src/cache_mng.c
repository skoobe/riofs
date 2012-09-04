#include "include/cache_mng.h"

struct _CacheMng {
    Application *app;
    gchar *path;
    GHashTable *h_cache_by_name;
    GHashTable *h_cache_by_inode;
};

typedef struct {
    gchar *name;
    guint64 inode;
    FILE *f;
    gboolean locked;
} CacheEntry;

CacheMng *cache_mng_create (Application *app, const gchar *path)
{
    CacheMng *mng;

    mng = g_new0 (CacheMng, 1);
    mng->app = app;
    mng->path = g_strdup (path);
    mng->h_cache_by_name = g_hash_table_new (g_str_hash, g_str_equal);
    mng->h_cache_by_inode = g_hash_table_new (g_direct_hash, g_direct_equal);

    return mng;
}

void cache_mng_destroy (CacheMng *mng)
{
    g_hash_table_destroy (mng->h_cache_by_name);
    g_hash_table_destroy (mng->h_cache_by_inode);
    g_free (mng->path);
    g_free (mng);
}

const gchar *cache_mng_add_file (CacheMng *mng, FILE *f)
{
}
