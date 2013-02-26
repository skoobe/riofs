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

static void range_test_setup (Range **range, gconstpointer test_data)
{
    *range = range_create ();
}

static void range_test_destroy (Range **range, gconstpointer test_data)
{
    range_destroy (*range);
}

static void range_test_add (Range **range, gconstpointer test_data)
{
    range_add (*range, 1, 10);
    g_assert (range_contain (*range, 2, 5) == TRUE);
    g_assert (range_count (*range) == 1);
    g_assert (range_length (*range) == 10);
}

static void range_test_extend_1 (Range **range, gconstpointer test_data)
{
    range_add (*range, 1, 10);
    range_add (*range, 2, 12);
    g_assert (range_contain (*range, 2, 12) == TRUE);
    g_assert (range_count (*range) == 1);
}

static void range_test_extend_2 (Range **range, gconstpointer test_data)
{
    range_add (*range, 1, 10);
    range_add (*range, 2, 12);
    range_add (*range, 10, 20);
    range_add (*range, 1, 50);
    range_add (*range, 60, 70);
    range_add (*range, 4, 5);
    range_add (*range, 7, 52);
    g_assert (range_contain (*range, 2, 12) == TRUE);
    g_assert (range_count (*range) == 2);
    g_assert (range_length (*range) == 52 + 11);
}


static void range_test_remove_1 (Range **range, gconstpointer test_data)
{
    range_add (*range, 1, 10);
    range_add (*range, 11, 15);
    range_add (*range, 2, 14);
    g_assert (range_contain (*range, 2, 14) == TRUE);
    g_assert (range_count (*range) == 1);
}

static void range_test_remove_2 (Range **range, gconstpointer test_data)
{
    range_add (*range, 1, 9);
    range_add (*range, 11, 15);
    range_add (*range, 16, 20);
    range_add (*range, 25, 30);
    range_add (*range, 25, 30); 
    range_add (*range, 32, 36); 
    range_add (*range, 40, 50); 


    range_add (*range, 10, 32); 
    g_assert (range_contain (*range, 2, 14) == FALSE);
    g_assert (range_contain (*range, 1, 9) == TRUE);
    g_assert (range_contain (*range, 10, 35) == TRUE);
    g_assert (range_count (*range) == 3);
}

static void range_test_remove_3 (Range **range, gconstpointer test_data)
{
    range_add (*range, 1, 9);
    range_add (*range, 40, 50);

    range_add (*range, 20, 25);
    range_add (*range, 15, 23);

    range_add (*range, 24, 30);

    range_add (*range, 10, 35); 

    g_assert (range_contain (*range, 2, 14) == FALSE);
    g_assert (range_contain (*range, 1, 9) == TRUE);
    g_assert (range_contain (*range, 10, 35) == TRUE);
    g_assert (range_count (*range) == 3);
}


int main (int argc, char *argv[])
{
    g_test_init (&argc, &argv, NULL);

	g_test_add ("/range/range_test_add", Range *, 0, range_test_setup, range_test_add, range_test_destroy);
	g_test_add ("/range/range_test_add", Range *, 0, range_test_setup, range_test_extend_1, range_test_destroy);
	g_test_add ("/range/range_test_add", Range *, 0, range_test_setup, range_test_extend_2, range_test_destroy);
	g_test_add ("/range/range_test_add", Range *, 0, range_test_setup, range_test_remove_1, range_test_destroy);
	g_test_add ("/range/range_test_add", Range *, 0, range_test_setup, range_test_remove_2, range_test_destroy);
	g_test_add ("/range/range_test_add", Range *, 0, range_test_setup, range_test_remove_3, range_test_destroy);

    return g_test_run ();
}
