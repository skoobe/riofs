#include "include/global.h"
#include "include/bucket_connection.h"
#include "include/dir_tree.h"
#include "include/fuse.h"

struct _Application {
    struct event_base *evbase;
    struct evdns_base *dns_base;
    
    S3Fuse *s3fuse;
    DirTree *dir_tree;

    gchar *aws_access_key_id;
    gchar *aws_secret_access_key;

    BucketConnection *con;
    S3Bucket *bucket;
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

BucketConnection *application_get_con (Application *app)
{
    return app->con;
}


void application_connected (Application *app, BucketConnection *con)
{
    bucket_connection_get_directory_listing (con, "/");
}

int main (int argc, char *argv[])
{
    Application *app;

    // init libraries
    ENGINE_load_builtin_engines ();
    ENGINE_register_all_complete ();

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
    
    app->bucket = g_new0 (S3Bucket, 1);
    app->bucket->uri = evhttp_uri_parse (argv[1]);
    app->bucket->name = g_strdup (argv[2]);

    app->dir_tree = dir_tree_create (app);
    if (!app->dir_tree) {
        LOG_err ("Failed to create DirTree !");
        return -1;
    }

    argv += 2;
    argc -= 2;

    app->s3fuse = s3fuse_create (app, argc, argv);
    if (!app->s3fuse) {
        LOG_err ("Failed to create FUSE fs !");
        return -1;
    }


    app->aws_access_key_id = getenv("AWSACCESSKEYID");
    app->aws_secret_access_key = getenv("AWSSECRETACCESSKEY");

    if (!app->aws_access_key_id || !app->aws_secret_access_key) {
        LOG_err ("Please set both AWSACCESSKEYID and AWSSECRETACCESSKEY environment variables !");
        return -1;
    }

    app->con = bucket_connection_new (app, app->bucket);
    bucket_connection_get_directory_listing (app->con, "/");

    event_base_dispatch (app->evbase);

    return 0;
}
