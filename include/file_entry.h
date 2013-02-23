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
#ifndef _FILE_ENTRY_H_
#define _FILE_ENTRY_H_

#include "global.h"

typedef struct _FileEntry FileEntry;

FileEntry *file_entry_create ();
void file_entry_destroy (FileEntry *fentry);

void file_entry_release (FileEntry *fentry);

typedef void (*FileEntry_on_buffer_written) (gpointer callback_data, gboolean success);
void file_entry_write_buffer (FileEntry *fentry,
    const char *buf, size_t buf_size, off_t off,
    FileEntry_on_buffer_written on_buffer_written, gpointer callback_data);

typedef void (*FileEntry_on_buffer_read) (gpointer callback_data, gboolean success, char *buf, size_t size);
void file_entry_read_buffer (FileEntry *fentry,
    size_t size, off_t off,
    FileEntry_on_buffer_read on_buffer_read, gpointer callback_data);

#endif
