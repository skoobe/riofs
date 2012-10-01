#ifndef _DIR_TREE_H_
#define _DIR_TREE_H_

#include "include/global.h"

typedef enum {
    DET_dir = 0,
    DET_file = 1,
} DirEntryType;

DirTree *dir_tree_create (Application *app);
void dir_tree_destroy (DirTree *dtree);

void dir_tree_update_entry (DirTree *dtree, const gchar *path, DirEntryType type, fuse_ino_t parent_ino, const gchar *entry_name, long long size);

void dir_tree_start_update (DirTree *dtree, const gchar *dir_path);
void dir_tree_stop_update (DirTree *dtree, fuse_ino_t parent_ino);

typedef void (*dir_tree_readdir_cb) (fuse_req_t req, gboolean success, size_t max_size, off_t off, const char *buf, size_t buf_size);
void dir_tree_fill_dir_buf (DirTree *dtree, 
        fuse_ino_t ino, size_t size, off_t off,
        dir_tree_readdir_cb readdir_cb, fuse_req_t req);

typedef void (*dir_tree_lookup_cb) (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size, time_t ctime);
void dir_tree_lookup (DirTree *dtree, fuse_ino_t parent_ino, const char *name,
    dir_tree_lookup_cb lookup_cb, fuse_req_t req);


typedef void (*dir_tree_getattr_cb) (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size, time_t ctime);
void dir_tree_getattr (DirTree *dtree, fuse_ino_t ino, 
    dir_tree_getattr_cb getattr_cb, fuse_req_t req);

typedef void (*dir_tree_setattr_cb) (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size);
void dir_tree_setattr (DirTree *dtree, fuse_ino_t ino, 
    struct stat *attr, int to_set,
    dir_tree_setattr_cb setattr_cb, fuse_req_t req, void *fi);


typedef void (*DirTree_file_read_cb) (fuse_req_t req, gboolean success, const char *buf, size_t buf_size);
void dir_tree_file_read (DirTree *dtree, fuse_ino_t ino, 
    size_t size, off_t off,
    DirTree_file_read_cb getattr_cb, fuse_req_t req,
    struct fuse_file_info *fi);

typedef void (*DirTree_file_create_cb) (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size, struct fuse_file_info *fi);
void dir_tree_file_create (DirTree *dtree, fuse_ino_t parent_ino, const char *name, mode_t mode,
    DirTree_file_create_cb file_create_cb, fuse_req_t req, struct fuse_file_info *fi);

typedef void (*DirTree_file_write_cb) (fuse_req_t req, gboolean success, size_t count);
void dir_tree_file_write (DirTree *dtree, fuse_ino_t ino, 
    const char *buf, size_t size, off_t off, 
    DirTree_file_write_cb file_write_cb, fuse_req_t req,
    struct fuse_file_info *fi);

typedef void (*DirTree_file_open_cb) (fuse_req_t req, gboolean success, struct fuse_file_info *fi);
gboolean dir_tree_file_open (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi, DirTree_file_open_cb file_open_cb, fuse_req_t req);

void dir_tree_file_release (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi);

typedef void (*DirTree_file_remove_cb) (fuse_req_t req, gboolean success);
gboolean dir_tree_file_remove (DirTree *dtree, fuse_ino_t ino, DirTree_file_remove_cb file_remove_cb, fuse_req_t req);


typedef void (*dir_tree_mkdir_cb) (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size, time_t ctime);
void dir_tree_dir_create (DirTree *dtree, fuse_ino_t parent_ino, const char *name, mode_t mode,
     dir_tree_mkdir_cb mkdir_cb, fuse_req_t req);


#endif
