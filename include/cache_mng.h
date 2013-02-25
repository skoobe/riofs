/*
 * Copyright (C) 2012-2013 Paul Ionkin <paul.ionkin@gmail.com>
 * Copyright (C) 2012-2013 Skoobe GmbH. All rights reserved.
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
#ifndef _CACHE_MNG_H_
#define _CACHE_MNG_H_

#include "global.h"

CacheMng *cache_mng_create (Application *app);
void cache_mng_destroy (CacheMng *cmng);

// retrieve file buffer from local storage
// if success == TRUE then "buf" contains "size" bytes of data
typedef void (*cache_mng_on_retrieve_file_buf_cb) (unsigned char *buf, size_t size, gboolean success, void *ctx);
void cache_mng_retrieve_file_buf (CacheMng *cmng, fuse_ino_t ino, size_t size, off_t off, 
    cache_mng_on_retrieve_file_buf_cb on_retrieve_file_buf_cb, void *ctx);

// store file buffer into local storage
// if success == TRUE then "buf" successfuly stored on disc
typedef void (*cache_mng_on_store_file_buf_cb) (gboolean success, void *ctx);
void cache_mng_store_file_buf (CacheMng *cmng, fuse_ino_t ino, size_t size, off_t off, unsigned char *buf,
        cache_mng_on_store_file_buf_cb on_store_file_buf_cb, void *ctx);

// removes file from local storage
void cache_mng_remove_file (CacheMng *cmng, fuse_ino_t ino);

#endif
