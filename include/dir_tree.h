#ifndef _DIR_TREE_H_
#define _DIR_TREE_H_

#include "include/global.h"


DirTree *dir_tree_create (Application *app);
void dir_tree_destroy (DirTree *dtree);

void dir_tree_update_entry (DirTree *dtree, const gchar *path, const gchar *entry_name, long long size);



typedef void (*dir_tree_readdir_cb) (gpointer readdir_cb_data, int result, const char *buf, size_t size);

gboolean dir_tree_fill_dir_buf (DirTree *dtree, 
        fuse_ino_t ino, size_t size, off_t off,
        dir_tree_readdir_cb, gpointer readdir_cb_data);


#endif
