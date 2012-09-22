#include "include/global.h"
#include "include/s3http_connection.h"
#include "include/dir_tree.h"
#include "include/s3fuse.h"

struct _Application {
    struct event_base *evbase;
    struct evdns_base *dns_base;
    
    S3Fuse *s3fuse;
    DirTree *dir_tree;
    S3HTTPClientPool *s3http_client_pool;
    S3HTTPConnection *s3http_connection;

    gchar *aws_access_key_id;
    gchar *aws_secret_access_key;

    gchar *bucket_name;
    struct evhttp_uri *url;
    gchar *s_url; // original uri string
};

struct event_base *application_get_evbase (Application *app)
{
    return app->evbase;
}

struct evdns_base *application_get_dnsbase (Application *app)
{
    return app->dns_base;
}

DirTree *application_get_dir_tree (Application *app)
{
    return app->dir_tree;
}

const gchar *application_get_access_key_id (Application *app)
{
    return (const gchar *) app->aws_access_key_id;
}

const gchar *application_get_secret_access_key (Application *app)
{
    return (const gchar *) app->aws_secret_access_key;
}

S3HTTPConnection *application_get_con (Application *app)
{
    return app->con;
}

CacheMng *application_get_cache_mng (Application *app)
{
    return app->cache_mng;
}

void application_connected (Application *app, S3HTTPConnection *con)
{
    s3http_connection_get_directory_listing (con, "/");
}

int main (int argc, char *argv[])
{
    Application *app;

    // init libraries
    ENGINE_load_builtin_engines ();
    ENGINE_register_all_complete ();

    // init main app structure
    app = g_new0 (Application, 1);
    app->evbase = event_base_new ();

    if (!app->evbase) {
        LOG_err ("Failed to create event base !");
        return -1;
    }

    app->dns_base = evdns_base_new (app->evbase, 1);
    if (!app->dns_base) {
        LOG_err ("Failed to create DNS base !");
        return -1;
    }

    // get access parameters from the enviorment
    // XXX: extend it
    app->aws_access_key_id = getenv("AWSACCESSKEYID");
    app->aws_secret_access_key = getenv("AWSSECRETACCESSKEY");

    if (!app->aws_access_key_id || !app->aws_secret_access_key) {
        LOG_err ("Please set both AWSACCESSKEYID and AWSSECRETACCESSKEY environment variables !");
        return -1;
    }

    // XXX: parse command line
    if (argc < 3) {
        LOG_err ("Please use s3ffs [http://s3.amazonaws.com] [bucketname] [FUSE params] [mountpoint]");
        return -1;
    }

    app->url = evhttp_uri_parse (argv[1]);
    if (!app->url) {
        LOG_err ("Failed to parse URL, please use s3ffs [http://s3.amazonaws.com] [bucketname] [FUSE params] [mountpoint]");
        return -1;
    }
    app->s_url = g_strdup (argv[1]);
    app->bucket_name = g_strdup (argv[2]);

    // create DirTree
    app->dir_tree = dir_tree_create (app);
    if (!app->dir_tree) {
        LOG_err ("Failed to create DirTree !");
        return -1;
    }
    
    // create FUSE
    argv += 2;
    argc -= 2;
    app->s3fuse = s3fuse_new (app, argc, argv);
    if (!app->s3fuse) {
        LOG_err ("Failed to create FUSE fs !");
        return -1;
    }
    
    // create S3HTTPClientPool
    app->s3http_client_pool = s3http_client_pool_create (app);
    if (!app->s3http_client_pool) {
        LOG_err ("Failed to create S3HTTPClientPool !");
        return -1;
    }

    // create S3HTTPConnection
    app->s3http_connection = s3http_connection_create (app, app->url, app->bucket_name);
    if (!app->s3http_connection) {
        LOG_err ("Failed to create S3HTTPConnection !");
        return -1;
    }

    // start the loop
    event_base_dispatch (app->evbase);

    return 0;
}
