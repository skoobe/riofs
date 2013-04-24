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
#ifndef _RIOFS_FUSE_H_
#define _RIOFS_FUSE_H_

#include "global.h"

struct dirbuf {
    char *p;
    size_t size;
};

RFuse *rfuse_new (Application *app, const gchar *mountpoint, const gchar *fuse_opts);
void rfuse_destroy (RFuse *rfuse);

void rfuse_add_dirbuf (fuse_req_t req, struct dirbuf *b, const char *name, fuse_ino_t ino, off_t file_size);

#endif
