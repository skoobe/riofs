#include "include/http_proto.h"

struct _HTTPConnection {
    Application *app;
};

// creates HTTPConnection object
HTTPConnection *http_connection_new (Application *app, const gchar *url)
{
    HTTPConnection *con;

    con = g_new0 (HTTPConnection, 1);
    if (!con) {
        LOG_err ("Failed to create HTTPConnection !");
        return NULL;
    }

    con->app = app;

    return app;
}


gboolean http_get_directory_listing (HTTPConnection *con, const gchar *path)
{
}
