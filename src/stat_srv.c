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
#include "stat_srv.h"
#include "utils.h"
#include "client_pool.h"
#include "dir_tree.h"
#include "rfuse.h"

struct _StatSrv {
    Application *app;
    ConfData *conf;

    struct evhttp *http;
    GQueue *q_op_history;
    time_t boot_time;
};

static struct PrintFormat print_format_http = {
    "<TABLE border=1>",      // header
    "</TABLE>",     // footer
    "<HEAD><TD><B>",   // caption_start
    "</B></TD></HEAD>", // caption_end
    "<TR><TD>",     // row_start
    "</TD></TR>",   // row_end
    "</TD><TD>",     // col_div
    "</B></TD><TD><B>"     // col_div for caption
};

static void stat_srv_on_stats_cb (struct evhttp_request *req, void *ctx);
static void stat_srv_on_gen_cb (struct evhttp_request *req, void *ctx);

#define STAT_LOG "stat"

StatSrv *stat_srv_create (Application *app)
{
    StatSrv *stat_srv;
    
    stat_srv = g_new0 (StatSrv, 1);
    stat_srv->app = app;
    stat_srv->conf = application_get_conf (app);
    stat_srv->q_op_history = g_queue_new ();
    stat_srv->boot_time = time (NULL);

    // stats server is disabled
    if (!conf_get_boolean (stat_srv->conf, "statistics.enabled")) {
        return stat_srv;
    }

    stat_srv->http = evhttp_new (application_get_evbase (app));
    if (!stat_srv->http) {
        LOG_err (STAT_LOG, "Failed to create statistics server !");
        return NULL;
    }

    // bind
    if (evhttp_bind_socket (stat_srv->http, 
        conf_get_string (stat_srv->conf, "statistics.host"),
        conf_get_int (stat_srv->conf, "statistics.port")) == -1) {
        LOG_err (STAT_LOG, "Failed to bind statistics server to  %s:%d",
            conf_get_string (stat_srv->conf, "statistics.host"),
            conf_get_int (stat_srv->conf, "statistics.port")
        );
        return NULL;
    }

    // install handlers
    evhttp_set_cb (stat_srv->http, conf_get_string (stat_srv->conf, "statistics.stats_path"), stat_srv_on_stats_cb, stat_srv);
    evhttp_set_gencb (stat_srv->http, stat_srv_on_gen_cb, stat_srv);

    return stat_srv;
}

void stat_srv_destroy (StatSrv *stat_srv)
{
    g_queue_free_full (stat_srv->q_op_history, g_free);

    if (stat_srv->http)
        evhttp_free (stat_srv->http);

    g_free (stat_srv);
}

void stats_srv_add_op_history (StatSrv *stat_srv, const gchar *str)
{
    // stats server is disabled
    if (!conf_get_boolean (stat_srv->conf, "statistics.enabled"))
        return;

    // maintain queue size
    while (g_queue_get_length (stat_srv->q_op_history) + 1 >= conf_get_uint (stat_srv->conf, "statistics.history_size")) {
        gchar *tmp = g_queue_pop_tail (stat_srv->q_op_history);
        g_free (tmp);
    }

    g_queue_push_head (stat_srv->q_op_history, g_strdup (str));
}

static void stat_srv_print_history_item (gchar *s_item, GString *str)
{
    g_string_append_printf (str, "%s<BR>", s_item);
}

static void stat_srv_on_stats_cb (struct evhttp_request *req, void *ctx)
{
    StatSrv *stat_srv = (StatSrv *) ctx;
    struct evbuffer *evb = NULL;
    const gchar *refresh = NULL;
    gint ref = 0;
    GString *str;
    struct evhttp_uri *uri;
    guint32 total_inodes, file_num, dir_num;
    guint64 read_ops, write_ops, dir_read_ops;
    const gchar *access_key = NULL;
    gboolean permitted = FALSE;

    uri = evhttp_uri_parse (evhttp_request_get_uri (req));
    LOG_debug (STAT_LOG, "Incoming request: %s", evhttp_request_get_uri (req));

    if (uri) {
        const gchar *query;
        
        query = evhttp_uri_get_query (uri);
        if (query) {
            struct evkeyvalq q_params;
            TAILQ_INIT (&q_params);
            evhttp_parse_query_str (query, &q_params);
            refresh = http_find_header (&q_params, "refresh");
            if (refresh)
                ref = atoi (refresh);
            access_key = http_find_header (&q_params, "access_key");
            if (access_key && !strcmp (access_key, conf_get_string (stat_srv->conf, "statistics.access_key")))
                permitted = TRUE;

            evhttp_clear_headers (&q_params);
        }
        evhttp_uri_free (uri);
    }

    if (!permitted) {
        evhttp_send_reply (req, HTTP_NOTFOUND, "Not found", NULL);
        return;
    }

    str = g_string_new (NULL);

    g_string_append_printf (str, "Uptime: %zd secs<BR>", time (NULL) - stat_srv->boot_time);

    // DirTree
    dir_tree_get_stats (application_get_dir_tree (stat_srv->app), &total_inodes, &file_num, &dir_num);
    g_string_append_printf (str, "<BR>DirTree: <BR>-Total inodes: %zu Total files: %zu Total directories: %zu<BR>",
        total_inodes, file_num, dir_num);

    // Fuse
    rfuse_get_stats (application_get_rfuse (stat_srv->app), &read_ops, &write_ops, &dir_read_ops);
    g_string_append_printf (str, "<BR>Fuse: <BR>-Read ops: %"G_GUINT64_FORMAT", Write ops: %"G_GUINT64_FORMAT
        " Dir read ops: %"G_GUINT64_FORMAT"<BR>",
        read_ops, write_ops, dir_read_ops);
    
    g_string_append_printf (str, "<BR>Read workers (%d): <BR>", 
        client_pool_get_client_count (application_get_read_client_pool (stat_srv->app)));
    client_pool_get_client_stats_info (application_get_read_client_pool (stat_srv->app), str, &print_format_http);
    
    g_string_append_printf (str, "<BR>Write workers (%d): <BR>",
        client_pool_get_client_count (application_get_write_client_pool (stat_srv->app)));
    client_pool_get_client_stats_info (application_get_write_client_pool (stat_srv->app), str, &print_format_http);
    
    g_string_append_printf (str, "<BR>Op workers (%d): <BR>",
        client_pool_get_client_count (application_get_ops_client_pool (stat_srv->app)));
    client_pool_get_client_stats_info (application_get_ops_client_pool (stat_srv->app), str, &print_format_http);

    g_string_append_printf (str, "<BR><BR>Operation history: <BR>");
    g_queue_foreach (stat_srv->q_op_history, (GFunc) stat_srv_print_history_item, str);

    evb = evbuffer_new ();

    evbuffer_add_printf (evb, "<HTTP>");
    if (ref) {
        evbuffer_add_printf (evb, "<HEAD><meta http-equiv=\"refresh\" content=\"%d\"></HEAD>", ref);
    }
    evbuffer_add_printf (evb, "<BODY>");
    evbuffer_add (evb, str->str, str->len);
    evbuffer_add_printf (evb, "</BODY></HTTP>");
    evhttp_send_reply (req, HTTP_OK, "OK", evb);
    evbuffer_free (evb);

    g_string_free (str, TRUE);
}

static void stat_srv_on_gen_cb (struct evhttp_request *req, void *ctx)
{
    StatSrv *stat_srv = (StatSrv *) ctx;
    const gchar *query;
    
    query = evhttp_uri_get_query (evhttp_request_get_evhttp_uri (req));
    
    LOG_debug (STAT_LOG, "Unhandled request to: %s", query);

    evhttp_send_reply (req, HTTP_NOTFOUND, "Not found", NULL);
}
