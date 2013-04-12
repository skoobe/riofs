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

struct _StatSrv {
    Application *app;
    ConfData *conf;

    struct evhttp *http;
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
    if (stat_srv->http)
        evhttp_free (stat_srv->http);

    g_free (stat_srv);
}

static void stat_srv_on_stats_cb (struct evhttp_request *req, void *ctx)
{
    StatSrv *stat_srv = (StatSrv *) ctx;
    struct evbuffer *evb = NULL;
    const gchar *refresh = NULL;
    gint ref = 0;
    const gchar *query;

    query = evhttp_uri_get_query (evhttp_request_get_evhttp_uri (req));
    if (query) {
        struct evkeyvalq q_params;
        TAILQ_INIT (&q_params);
        evhttp_parse_query_str (query, &q_params);
        refresh = http_find_header (&q_params, "refresh");
        if (refresh)
            ref = atoi (refresh);
        evhttp_clear_headers (&q_params);
    }

    evb = evbuffer_new ();

    if (refresh) {
        evbuffer_add_printf (evb, "<meta http-equiv=\"refresh\" content=\"%d\">", ref);
    }

    {
        evbuffer_add_printf (evb, "%s", "stats"
        );
    }

    evhttp_send_reply (req, 200, "OK", evb);
    evbuffer_free (evb);
}

static void stat_srv_on_gen_cb (struct evhttp_request *req, void *ctx)
{
    StatSrv *stat_srv = (StatSrv *) ctx;
    const gchar *query;
    
    query = evhttp_uri_get_query (evhttp_request_get_evhttp_uri (req));
    
    LOG_debug (STAT_LOG, "Unhandled request to: %s", query);
}
