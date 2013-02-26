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
#include "cache_mng.h"
#include "test_application.h"

struct test_ctx {
    gboolean success;
    unsigned char *buf;
    size_t buflen;
};

static Application *app;

static void cache_mng_test_setup (CacheMng **cmng, gconstpointer test_data)
{
    *cmng = cache_mng_create (app);
}

static void cache_mng_test_destroy (CacheMng **cmng, gconstpointer test_data)
{
    cache_mng_destroy (*cmng);
}

static void store_cb (gboolean success, void *ctx)
{
    struct test_ctx *test_ctx = (struct test_ctx *) ctx;

    test_ctx->success = success;
}

static void retrieve_cb (unsigned char *buf, size_t size, gboolean success, void *ctx)
{
    struct test_ctx *test_ctx = (struct test_ctx *) ctx;

    test_ctx->success = success;
    if (success) {
        test_ctx->buf = g_malloc (size);
        memcpy(test_ctx->buf, buf, size);
        test_ctx->buflen = size;
    }
}

static void cache_mng_test_store (CacheMng **cmng, gconstpointer test_data)
{
    struct test_ctx test_ctx = {FALSE, NULL, 0};
    int i;
    unsigned char buf[256];

    for (i = 0; i < (int) sizeof (buf); i++)
        buf[i] = i % 256;

    cache_mng_store_file_buf (*cmng, 1, 10, 0, buf, store_cb, &test_ctx);
    cache_mng_store_file_buf (*cmng, 1, 10, 10, buf + 10, store_cb, &test_ctx);
    cache_mng_store_file_buf (*cmng, 1, 20, 5, buf + 5, store_cb, &test_ctx);
    app_dispatch (app);

    g_assert (test_ctx.success);
    g_assert (cache_mng_size (*cmng) == 25);

    cache_mng_retrieve_file_buf (*cmng, 1, 25, 0, retrieve_cb, &test_ctx);
    app_dispatch (app);

    g_assert (test_ctx.success);
    g_assert (test_ctx.buflen == 25);
    g_assert (memcmp (test_ctx.buf, buf, test_ctx.buflen) == 0);

    g_free (test_ctx.buf);
}

static void cache_mng_test_remove (CacheMng **cmng, gconstpointer test_data)
{
    struct test_ctx test_ctx = {FALSE, NULL, 0};
    int i;
    unsigned char buf[256];

    for (i = 0; i < (int) sizeof (buf); i++)
        buf[i] = i % 256;

    cache_mng_store_file_buf (*cmng, 1, sizeof (buf), 0, buf, store_cb, &test_ctx);
    cache_mng_retrieve_file_buf (*cmng, 1, 1, 0, retrieve_cb, &test_ctx);
    app_dispatch (app);

    g_assert (test_ctx.success);
    g_assert (cache_mng_size (*cmng) == sizeof (buf));
    g_free (test_ctx.buf);

    cache_mng_remove_file (*cmng, 1);
    cache_mng_retrieve_file_buf (*cmng, 1, 1, 0, retrieve_cb, &test_ctx);
    app_dispatch (app);
    g_assert (!test_ctx.success);
    g_assert (cache_mng_size (*cmng) == 0);
}

static void cache_mng_test_lru (CacheMng **cmng, gconstpointer test_data)
{
    struct test_ctx test_ctx = {FALSE, NULL, 0};
    int i;
    unsigned char buf[50];

    for (i = 0; i < (int) sizeof (buf); i++)
        buf[i] = i % 256;

    cache_mng_store_file_buf (*cmng, 1, sizeof (buf), 0, buf, store_cb, &test_ctx);
    cache_mng_store_file_buf (*cmng, 2, sizeof (buf), 0, buf, store_cb, &test_ctx);
    cache_mng_retrieve_file_buf (*cmng, 1, 1, 0, retrieve_cb, &test_ctx);
    app_dispatch (app);

    g_assert (test_ctx.success);
    g_assert (cache_mng_size (*cmng) == 100);
    g_free (test_ctx.buf);

    cache_mng_store_file_buf (*cmng, 3, sizeof (buf), 0, buf, store_cb, &test_ctx);
    app_dispatch (app);

    g_assert (test_ctx.success);
    g_assert (cache_mng_size (*cmng) == 100);

    cache_mng_retrieve_file_buf (*cmng, 2, 1, 0, retrieve_cb, &test_ctx);
    app_dispatch (app);

    g_assert (!test_ctx.success);
}

int main (int argc, char *argv[])
{
    app = app_create ();
    g_test_init (&argc, &argv, NULL);

	g_test_add ("/cache_mng/cache_mng_test_store", CacheMng *, 0, cache_mng_test_setup, cache_mng_test_store, cache_mng_test_destroy);
	g_test_add ("/cache_mng/cache_mng_test_remove", CacheMng *, 0, cache_mng_test_setup, cache_mng_test_remove, cache_mng_test_destroy);
	g_test_add ("/cache_mng/cache_mng_test_lru", CacheMng *, 0, cache_mng_test_setup, cache_mng_test_lru, cache_mng_test_destroy);

    return g_test_run ();
}
