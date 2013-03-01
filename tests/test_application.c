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
#include "test_application.h"

struct event_base *application_get_evbase (Application *app)
{
    return app->evbase;
}

struct evdns_base *application_get_dnsbase (Application *app)
{
    return app->dns_base;
}

gboolean application_set_url (Application *app, const gchar *url)
{
    return TRUE;
}

ConfData *application_get_conf (Application *app)
{
    return app->conf;
}

Application *app_create ()
{
    Application *app = g_new0 (Application, 1);
    app->evbase = event_base_new ();
    app->dns_base = NULL;
    app->conf = conf_create ();

    conf_set_boolean (app->conf, "filesystem.cache_enabled", TRUE);
    conf_set_string (app->conf, "filesystem.cache_dir", "/tmp/s3ffs");
    conf_set_string (app->conf, "filesystem.cache_dir_max_size", "1Gb");
    
    return app;
}

void app_enable_dns(Application *app)
{
    app->dns_base = evdns_base_new (app->evbase, 1);
}

void app_dispatch(Application *app)
{
    event_base_dispatch (app->evbase);
}

void app_destroy (Application *app)
{
    g_free (app);
}
