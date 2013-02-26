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
    app->dns_base = evdns_base_new (app->evbase, 1);
    app->conf = conf_create ();

    conf_add_boolean (app->conf, "filesystem.cache_enabled", TRUE);
    conf_add_string (app->conf, "filesystem.cache_dir", "/tmp/s3ffs");
    conf_add_string (app->conf, "filesystem.cache_dir_max_size", "1Gb");
    
    return app;
}

void app_dispatch(Application *app)
{
    event_base_dispatch (app->evbase);
}

void app_destroy (Application *app)
{
    g_free (app);
}
