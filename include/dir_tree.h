#ifndef _DIR_TREE_H_
#define _DIR_TREE_H_

#include "include/global.h"


DirTree *dir_tree_create (Application *app);
void dir_tree_destroy (DirTree *dtree);

void dir_tree_update_entry (DirTree *dtree, const gchar *path, const gchar *entry_name, long long size);



typedef void (*dir_tree_readdir_cb) (fuse_req_t req, gboolean success, size_t max_size, off_t off, const char *buf, size_t buf_size);
void dir_tree_fill_dir_buf (DirTree *dtree, 
        fuse_ino_t ino, size_t size, off_t off,
        dir_tree_readdir_cb readdir_cb, fuse_req_t req);

typedef void (*dir_tree_lookup_cb) (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size);
void dir_tree_lookup (DirTree *dtree, fuse_ino_t parent_ino, const char *name,
    dir_tree_lookup_cb lookup_cb, fuse_req_t req);


typedef void (*dir_tree_getattr_cb) (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size);
void dir_tree_getattr (DirTree *dtree, fuse_ino_t ino, 
    dir_tree_getattr_cb getattr_cb, fuse_req_t req);

typedef void (*dir_tree_setattr_cb) (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size);
void dir_tree_setattr (DirTree *dtree, fuse_ino_t ino, 
    struct stat *attr, int to_set,
    dir_tree_setattr_cb setattr_cb, fuse_req_t req, void *fi);


typedef void (*dir_tree_read_cb) (fuse_req_t req, gboolean success, size_t max_size, off_t off, const char *buf, size_t buf_size);
void dir_tree_read (DirTree *dtree, fuse_ino_t ino, 
    size_t size, off_t off,
    dir_tree_read_cb getattr_cb, fuse_req_t req);

typedef void (*dir_tree_add_file_cb) (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size, void *fi);
void dir_tree_add_file (DirTree *dtree, fuse_ino_t parent_ino, const char *name, mode_t mode,
    dir_tree_add_file_cb add_file_cb, fuse_req_t req, void *fi);
#endif
