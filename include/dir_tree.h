/*
 * Copyright (C) 2012-2014 Paul Ionkin <paul.ionkin@gmail.com>
 * Copyright (C) 2012-2014 Skoobe GmbH. All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */
#ifndef _DIR_TREE_H_
#define _DIR_TREE_H_

#include "global.h"

typedef enum {
    DET_dir = 0,
    DET_file = 1,
} DirEntryType;

typedef struct _DirEntry DirEntry;

DirTree *dir_tree_create (Application *app);
void dir_tree_destroy (DirTree *dtree);

DirEntry *dir_tree_update_entry (DirTree *dtree, const gchar *path, DirEntryType type,
    fuse_ino_t parent_ino, const gchar *entry_name, long long size, time_t last_modified);

void dir_tree_entry_update_xattrs (DirEntry *en, struct evkeyvalq *headers);

// mark that DirTree is being updated

void dir_tree_start_update (DirEntry *en, G_GNUC_UNUSED const gchar *dir_path);
void dir_tree_stop_update (DirTree *dtree, fuse_ino_t parent_ino);

gboolean dir_tree_opendir (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi);
gboolean dir_tree_releasedir (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi);

typedef void (*dir_tree_readdir_cb) (fuse_req_t req, gboolean success, size_t max_size, off_t off,
    const char *buf, size_t buf_size,
    gpointer ctx);
void dir_tree_fill_dir_buf (DirTree *dtree,
    fuse_ino_t ino, size_t size, off_t off,
    dir_tree_readdir_cb readdir_cb, fuse_req_t req,
    gpointer ctx, struct fuse_file_info *fi);

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
void dir_tree_file_open (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi, DirTree_file_open_cb file_open_cb, fuse_req_t req);

void dir_tree_file_release (DirTree *dtree, fuse_ino_t ino, struct fuse_file_info *fi);

typedef void (*DirTree_file_remove_cb) (fuse_req_t req, gboolean success);
void dir_tree_file_remove (DirTree *dtree, fuse_ino_t ino, DirTree_file_remove_cb file_remove_cb, fuse_req_t req);
void dir_tree_file_unlink (DirTree *dtree, fuse_ino_t parent_ino, const char *name, DirTree_file_remove_cb file_remove_cb, fuse_req_t req);

gboolean dir_tree_dir_remove (DirTree *dtree, fuse_ino_t parent_ino, const char *name, fuse_req_t req);

typedef void (*dir_tree_mkdir_cb) (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size, time_t ctime);
void dir_tree_dir_create (DirTree *dtree, fuse_ino_t parent_ino, const char *name, mode_t mode,
     dir_tree_mkdir_cb mkdir_cb, fuse_req_t req);

typedef void (*DirTree_rename_cb) (fuse_req_t req, gboolean success);
void dir_tree_rename (DirTree *dtree,
    fuse_ino_t parent_ino, const char *name, fuse_ino_t newparent_ino, const char *newname,
    DirTree_rename_cb rename_cb, fuse_req_t req);

typedef void (*dir_tree_getxattr_cb) (fuse_req_t req, gboolean success, fuse_ino_t ino, const gchar *str, size_t size);
void dir_tree_getxattr (DirTree *dtree, fuse_ino_t ino,
    const char *name, size_t size,
    dir_tree_getxattr_cb getxattr_cb, fuse_req_t req);

void dir_tree_get_stats (DirTree *dtree, guint32 *total_inodes, guint32 *file_num, guint32 *dir_num);

guint dir_tree_get_inode_count (DirTree *dtree);

void dir_tree_set_entry_exist (DirTree *dtree, fuse_ino_t ino);


typedef void (*DirTree_symlink_cb) (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size, time_t ctime);
void dir_tree_create_symlink (DirTree *dtree, fuse_ino_t parent_ino, const char *fname, const char *link,
    DirTree_symlink_cb symlink_cb, fuse_req_t req);

typedef void (*DirTree_readlink_cb) (fuse_req_t req, gboolean success, fuse_ino_t ino, const char *link);
void dir_tree_readlink (DirTree *dtree, fuse_ino_t ino, DirTree_readlink_cb readlink_cb, fuse_req_t req);
#endif
