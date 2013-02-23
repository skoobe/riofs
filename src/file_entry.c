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
#include "file_entry.h"

/*{{{ struct */
struct _FileEntry {
    gboolean released; // a simple version of ref counter.
    size_t current_size; // total bytes after read / write calls
    struct evbuffer *piece_buf; // current piece buffer
    size_t piece_max_size; // piece max size
};
/*}}}*/

#define FENTRY_LOG "fentry"

FileEntry *file_entry_create ()
{
    FileEntry *fentry;

    fentry = g_new0 (FileEntry, 1);
    fentry->released = FALSE;
    fentry->current_size = 0;
    fentry->piece_buf = evbuffer_new ();
    fentry->piece_max_size = 1024 * 1024 * 10; // 10 mb

    return fentry;
}

void file_entry_destroy (FileEntry *fentry)
{
    g_free (fentry);
}

// file is released, finish all operations
void file_entry_release (FileEntry *fentry)
{
    fentry->released = TRUE;
}

// Add data to file entry piece buffer
// if piece buffer exceeds MAX size then send piece buffer to server
// execute callback function when data either is sent or added to buffer
void file_entry_write_buffer (FileEntry *fentry,
    const char *buf, size_t buf_size, off_t off,
    FileEntry_on_buffer_written on_buffer_written, gpointer callback_data)
{
    // XXX: allow only sequentially write
    // current written bytes should be always match offset
    if (fentry->current_size != off) {
        LOG_err (FENTRY_LOG, "Write call with offset %"OFF_FMT" is not allowed !", off);
        on_buffer_written (callback_data, FALSE);
        return;
    }

    // XXX: add to CacheMng

    evbuffer_add (fentry->piece_buf, buf, buf_size);
    fentry->current_size += buf_size;
    
    // check if we need to flush piece
    if (evbuffer_get_length (fentry->piece_buf) >= fentry->piece_max_size) {
        // XXX: add task to ClientPool
    } else {
        // data is added to piece buffer
        on_buffer_written (callback_data, TRUE);
    }
}

// Send request to server to get piece buffer
void file_entry_read_buffer (FileEntry *fentry,
    size_t size, off_t off,
    FileEntry_on_buffer_read on_buffer_read, gpointer callback_data)
{
    // XXX: check file in CacheMng
    // XXX: add task to ClientPool
}
