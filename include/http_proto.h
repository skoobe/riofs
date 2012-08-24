#include "include/global.h"

HTTPConnection *http_connection_new (Application *app, const gchar *url);

gboolean http_get_directory_listing (HTTPConnection *con, const gchar *path);
