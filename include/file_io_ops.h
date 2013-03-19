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
#ifndef _FILE_IO_OPS_H_
#define _FILE_IO_OPS_H_

#include "global.h"

typedef struct _FileIO FileIO;

FileIO *fileio_create (Application *app, const gchar *fname, fuse_ino_t ino);
void fileio_destroy (FileIO *fop);

void fileio_release (FileIO *fop);

typedef void (*FileIO_on_buffer_written_cb) (FileIO *fop, gpointer ctx, gboolean success, size_t count);
void fileio_write_buffer (FileIO *fop,
    const char *buf, size_t buf_size, off_t off, fuse_ino_t ino,
    FileIO_on_buffer_written_cb on_buffer_written_cb, gpointer ctx);

typedef void (*FileIO_on_buffer_read_cb) (gpointer ctx, gboolean success, char *buf, size_t size);
void fileio_read_buffer (FileIO *fop,
    size_t size, off_t off, fuse_ino_t ino,
    FileIO_on_buffer_read_cb on_buffer_read_cb, gpointer ctx);

#endif
