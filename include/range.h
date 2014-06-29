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
#ifndef _RANGE_H_
#define _RANGE_H_

#include "global.h"

typedef struct _Range Range;

Range *range_create ();

void range_destroy (Range *range);

void range_add (Range *range, guint64 start, guint64 end);

gboolean range_contain (Range *range, guint64 start, guint64 end);
gint range_count (Range *range);
guint64 range_length (Range *range);
void range_print (Range *range);

#endif
