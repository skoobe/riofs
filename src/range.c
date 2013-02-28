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
#include "range.h"

struct _Range {
    GList *l_intervals; // sorted list of intervals
};

typedef struct {
    guint64 start;
    guint64 end;
} Interval;

Range *range_create ()
{
    Range *range;

    range = g_new0 (Range, 1);
    range->l_intervals = NULL;

    return range;
}

void range_destroy (Range *range)
{
    GList *l;

    for (l = g_list_first (range->l_intervals); l; l = g_list_next (l)) {
        Interval *in = (Interval *) l->data;
        g_free (in);
    }
    g_list_free (range->l_intervals);
    g_free (range);
}

static gint intervals_compare (Interval *a, Interval *b)
{
    if (a->start < b->start)
        return -1;
    else if (a->start > b->start)
        return 1;
    else 
        return 0;
}

void range_add (Range *range, guint64 start, guint64 end)
{
    GList *l;
    gboolean found = FALSE;

    l = g_list_first (range->l_intervals);
    while (l) {
        Interval *in = (Interval *) l->data;
        
        // is in range
        if (in->start <= start && in->end >= end) {
            return;
        }

        // overlaps
        if ((in->start >= start && in->start <= end) || (in->end >= start && in->end <= end)) {
            GList *j;
            found = TRUE;
            // extend it
            if (in->end < end)
                in->end = end;
            if (in->start > start)
                in->start = start;

            
            j = l->next;
            while (j) {
                // extend
                Interval *in1 = (Interval *) j->data;
                if ((in->start >= in1->start && in->start <= in1->end) || 
                    (in->end >= in1->start && in->end <= in1->end) || 
                    (in->start <= in1->start && in->end >= in1->end)) {
                    GList *l_save;

                    if (in->end < in1->end)
                        in->end = in1->end;
                    if (in->start > in1->start)
                        in->start = in1->start;

                    l_save = j->next;
                    range->l_intervals = g_list_delete_link (range->l_intervals, j);
                    j = l_save;

                    g_free (in1);
                } else 
                    j = j->next;
            }
        }

        l = l->next;
    }

    // not found
    if (!found) {
        Interval *in = g_new0 (Interval, 1);
        in->start = start;
        in->end = end;
        range->l_intervals = g_list_insert_sorted (range->l_intervals, in, (GCompareFunc) intervals_compare);
    }
}

gboolean range_contain (Range *range, guint64 start, guint64 end)
{
    GList *l;

    for (l = g_list_first (range->l_intervals); l; l = g_list_next (l)) {
        Interval *in = (Interval *) l->data;

        if (in->start <= start && in->end >= end)
            return TRUE;
    }

    return FALSE;
}

gint range_count (Range *range)
{
    return g_list_length (range->l_intervals);
}

guint64 range_length (Range *range)
{
    GList *l;
    guint64 length = 0;

    for (l = g_list_first (range->l_intervals); l; l = g_list_next (l)) {
        Interval *in = (Interval *) l->data;

        g_assert (in->start <= in->end);
        length += in->end - in->start;
    }

    return length;
}

void range_print (Range *range)
{
    GList *l;

    g_printf ("===\n");
    for (l = g_list_first (range->l_intervals); l; l = g_list_next (l)) {
        Interval *in = (Interval *) l->data;
        g_printf ("[%"G_GUINT64_FORMAT" %"G_GUINT64_FORMAT"]\n", in->start, in->end);
    }
}
