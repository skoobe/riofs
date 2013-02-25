#ifndef _TEST_APP_H_
#define _TEST_APP_H_

#include "global.h"

struct _Application {
    struct event_base *evbase;
    struct evdns_base *dns_base;
    ConfData *conf;

    GList *l_files;
    GHashTable *h_clients_freq; // keeps the number of requests for each HTTP client
};

Application *app_create ();
void app_destroy (Application *app);

#endif
