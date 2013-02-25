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
#include "global.h"
#include "test_application.h"
#include "s3http_client.h"
#include "s3http_connection.h"
#include "s3client_pool.h"
#include "utils.h"

#define POOL_TEST "pool_test"
typedef struct {
    gchar *in_name;
    gchar *md5;
    gchar *url;
    
    gchar *out_name;
    FILE *fout;

    gboolean checked;
    GList *l;
} FileData;

typedef struct {
    S3ClientPool *pool;
    GList *l_files;
} CBData;

static Application *app;

// create max_files and fill with random data
// return list of {file name, content md5}
static GList *populate_file_list (gint max_files, GList *l_files, gchar *in_dir)
{
    gint i;
    gchar *out_dir;
    GError *error = NULL;
    FileData *fdata;
    gchar *name;
    FILE *f;

    out_dir = g_dir_make_tmp (NULL, &error);
    g_assert (out_dir);


    LOG_debug (POOL_TEST, "In dir: %s   Out dir: %s", in_dir, out_dir);

    for (i = 0; i < max_files; i++) {
        char *bytes;
        size_t bytes_len;

        fdata = g_new0 (FileData, 1);
        fdata->checked = FALSE;
        bytes_len = g_random_int_range (100000, 1000000);
        bytes = g_malloc (bytes_len + 1);
        RAND_pseudo_bytes (bytes, bytes_len);
        *(bytes + bytes_len) = '\0';
        
        name = get_random_string (15, TRUE);
        fdata->in_name = g_strdup_printf ("%s/%s", in_dir, name);
        f = fopen (fdata->in_name, "w");
        fwrite (bytes, 1, bytes_len + 1, f);
        fclose (f);

        fdata->out_name = g_strdup_printf ("%s/%s", out_dir, name);
        fdata->md5 = get_md5_sum (bytes, bytes_len + 1);
        
        fdata->fout = fopen (fdata->out_name, "w");
        g_assert (fdata->fout);

        fdata->url = g_strdup_printf ("http://127.0.0.1:8011/%s", name);
        g_assert (fdata->url);
        
        LOG_debug (POOL_TEST, "%s -> %s, size: %u", fdata->in_name, fdata->md5, bytes_len);
        
        l_files = g_list_append (l_files, fdata);
    }

    return l_files;
}

gboolean check_list (GList *l)
{
    GList *tmp;
    
    for (tmp = g_list_first (l); tmp; tmp = g_list_next (tmp)) {
        FileData *fdata = (FileData *) tmp->data;
        if (!fdata->checked)
            return FALSE;
    }
    return TRUE;
}

static void on_last_chunk_cb (S3HttpClient *http, struct evbuffer *input_buf, gpointer ctx)
{
    gchar *buf = NULL;
    size_t buf_len;
    FileData *fdata = (FileData *) ctx;
    gchar *md5;

    buf_len = evbuffer_get_length (input_buf);
    buf = (gchar *) evbuffer_pullup (input_buf, buf_len);

    md5 = get_md5_sum (buf, buf_len);

    LOG_debug (POOL_TEST, "%s == %s", fdata->md5, md5);
    g_assert_cmpstr (fdata->md5, ==, md5);

    fdata->checked = TRUE;

    s3http_client_release (http);

    if (check_list (app->l_files)) {
        event_base_loopbreak (app->evbase);
        LOG_debug (POOL_TEST, "Test passed !");
    }
}

static void on_get_http_client (S3HttpClient *http, gpointer pool_ctx)
{
    FileData *fd = (FileData *) pool_ctx;
    gpointer p;
    gint i;

    LOG_debug (POOL_TEST, "Got http client %p, sending request for: %s", http, fd->in_name);

    if ((p = g_hash_table_lookup (app->h_clients_freq, http)) != NULL) {
        i = GPOINTER_TO_INT (p) + 1;
        g_hash_table_replace (app->h_clients_freq, http, GINT_TO_POINTER (i));
    } else {
        i = 1;
        g_hash_table_insert (app->h_clients_freq, http, GINT_TO_POINTER (i));
    }

    s3http_client_acquire (http);

    s3http_client_request_reset (http);

    s3http_client_set_cb_ctx (http, fd);
    s3http_client_set_on_last_chunk_cb (http, on_last_chunk_cb);

    s3http_client_set_output_length (http, 0);
    s3http_client_start_request (http, S3Method_get, fd->url);
}

static void on_output_timer (evutil_socket_t fd, short event, void *ctx)
{
    gint i;
    GList *l;

    CBData *cb = (CBData *) ctx;
    S3ClientPool *pool = cb->pool;

    for (l = g_list_first (cb->l_files); l; l = g_list_next (l)) {
        FileData *fd = (FileData *) l->data;
        g_assert (s3client_pool_get_client (pool, on_get_http_client, fd));
    }
}

#define BUFFER_SIZE 1024 * 10

static void on_srv_request (struct evhttp_request *req, void *ctx)
{
    struct evbuffer *in;
    gchar *dir = (gchar *) ctx;
    gchar *path, *tmp, *decoded_path;
	const char *uri = evhttp_request_get_uri(req);
	struct evhttp_uri *decoded = NULL;
    struct evbuffer *evb = NULL;
    char buf[BUFFER_SIZE];
    FILE *f;
    size_t bytes_read;
    size_t total_bytes = 0;

    in = evhttp_request_get_input_buffer (req);

	decoded = evhttp_uri_parse(uri);
    g_assert (decoded);
	tmp = evhttp_uri_get_path(decoded);
    g_assert (tmp);
    decoded_path = evhttp_uridecode(tmp, 0, NULL);

    evb = evbuffer_new();

    path = g_strdup_printf ("%s/%s", dir, decoded_path);
    LOG_debug (POOL_TEST, "SRV: received %d bytes. Req: %s, path: %s", evbuffer_get_length (in), evhttp_request_get_uri (req), path);

    f = fopen (path, "r");
    g_assert (f);

    while ((bytes_read = fread (buf, 1, BUFFER_SIZE, f)) > 0) {
        evbuffer_add (evb, buf, bytes_read);
        total_bytes += bytes_read;
    }
    evhttp_send_reply(req, 200, "OK", evb);

    LOG_debug (POOL_TEST, "Total bytes sent: %u", total_bytes);

    fclose(f);
    evbuffer_free(evb);
}

static void start_srv (struct event_base *base, gchar *in_dir)
{
    struct evhttp *http;

    http = evhttp_new (base);
    evhttp_bind_socket (http, "127.0.0.1", 8011);
    evhttp_set_gencb (http, on_srv_request, in_dir);
}


static gboolean print_foreach (gconstpointer a, gconstpointer b)
{
    g_printf ("%p: %i\n", a, GPOINTER_TO_INT (b));
    return FALSE;
}

int main (int argc, char *argv[])
{
    S3ClientPool *pool;
    struct event *timeout;
    struct timeval tv;
    GList *l_files = NULL;
    CBData *cb;
    gchar *in_dir;

    log_level = LOG_debug;

    event_set_mem_functions (g_malloc, g_realloc, g_free);

    in_dir = g_dir_make_tmp (NULL, NULL);
    g_assert (in_dir);

    l_files = populate_file_list (100, l_files, in_dir);
    g_assert (l_files);

    app = app_create ();
    app->h_clients_freq = g_hash_table_new (g_direct_hash, g_direct_equal);
    app->l_files = l_files;
    // start server
    start_srv (app->evbase, in_dir);
    
    pool = s3client_pool_create (app, 12,
        s3http_client_create,
        s3http_client_destroy,
        s3http_client_set_on_released_cb,
        s3http_client_check_rediness
    );

    cb = g_new (CBData, 1);
    cb->pool = pool;
    cb->l_files = l_files;
    
    timeout = evtimer_new (app->evbase, on_output_timer, cb);

    evutil_timerclear(&tv);
    tv.tv_sec = 0;
    tv.tv_usec = 500;
    event_add (timeout, &tv);

    event_base_dispatch (app->evbase);

    g_hash_table_foreach (app->h_clients_freq, (GHFunc)print_foreach, NULL);

    return 0;
}
