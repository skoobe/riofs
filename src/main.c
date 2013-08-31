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
#include "http_connection.h"
#include "dir_tree.h"
#include "rfuse.h"
#include "utils.h"
#include "client_pool.h"
#include "cache_mng.h"
#include "stat_srv.h"
#include "conf_keys.h"

/*{{{ struct */
struct _Application {
    gchar *conf_path;
    ConfData *conf;
    struct event_base *evbase;
    struct evdns_base *dns_base;
    FILE *f_log;
    gchar *log_file_name;
    
    RFuse *rfuse;
    DirTree *dir_tree;
    CacheMng *cmng;
    StatSrv *stat_srv;

    // initial bucket ACL request
    HttpConnection *service_con;

    ClientPool *write_client_pool;
    ClientPool *read_client_pool;
    ClientPool *ops_client_pool;

    gchar *fuse_opts;

    struct evhttp_uri *uri;

    struct event *sigint_ev;
    struct event *sigterm_ev;
    struct event *sigpipe_ev;
    struct event *sigusr1_ev;
    struct event *sigusr2_ev;

#ifdef SSL_ENABLED
    SSL_CTX *ssl_ctx;
#endif
};

// global variable, used by signals handlers
static Application *_app = NULL;

#define APP_LOG "main"
/*}}}*/

/*{{{ getters */
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

RFuse *application_get_rfuse (Application *app)
{
    return app->rfuse;
}

ClientPool *application_get_write_client_pool (Application *app)
{
    return app->write_client_pool;
}

ClientPool *application_get_read_client_pool (Application *app)
{
    return app->read_client_pool;
}

ClientPool *application_get_ops_client_pool (Application *app)
{
    return app->ops_client_pool;
}

ConfData *application_get_conf (Application *app)
{
    return app->conf;
}

CacheMng *application_get_cache_mng (Application *app)
{
    return app->cmng;
}

StatSrv *application_get_stat_srv (Application *app)
{
    return app->stat_srv;
}

#ifdef SSL_ENABLED
SSL_CTX *application_get_ssl_ctx (Application *app)
{
    return app->ssl_ctx;
}
#endif

/*}}}*/

/*{{{ application_exit*/
void application_exit (Application *app)
{
    if (app->rfuse && rfuse_get_mounted(app->rfuse)) {
        /*
         * Unmount the volume before exiting the event loop. On OS X unmounting
         * the volume flushes the kernel's unified buffer cache, which results
         * in file system requests, that need to be handled by the event loop.
         * Exiting the event loop without unmounting the volume first can cause
         * data loss or corruption on OS X.
         *
         * application_exit () is called again after the unmount has finished.
         */
        rfuse_unmount (app->rfuse);
    } else
        event_base_loopexit (app->evbase, NULL);
}
/*}}}*/

/*{{{ application_set_url*/
gboolean application_set_url (Application *app, const gchar *url)
{
    if (app->uri)
        evhttp_uri_free (app->uri);
    
    // check if URL contains HTTP or HTTPS
    if (strlen (url) < 4 || !strcasestr (url, "http") || strcasestr (url, "http") != url) {
        // XXX: check config and decide HTTP or HTTPS ?
        gchar *tmp;

        tmp = g_strdup_printf ("http://%s", url);
        app->uri = evhttp_uri_parse (tmp);
        g_free (tmp);
    } else 
        app->uri = evhttp_uri_parse (url);


    if (!app->uri) {
        LOG_err (APP_LOG, " URL (%s) is not valid!", url);

        application_exit (app);
        return FALSE;
    }

    if (conf_get_boolean (app->conf, "s3.path_style")) {
        conf_set_string (app->conf, "s3.host", evhttp_uri_get_host (app->uri));
    } else {
        // add bucket name to s3.amazonaws.com
        if (!strcmp (evhttp_uri_get_host (app->uri), "s3.amazonaws.com")) {
            gchar *tmp = g_strdup_printf ("%s.s3.amazonaws.com", conf_get_string (app->conf, "s3.bucket_name"));
            conf_set_string (app->conf, "s3.host", tmp);
            g_free (tmp);
        } else
            conf_set_string (app->conf, "s3.host", evhttp_uri_get_host (app->uri));
    }

    conf_set_int (app->conf, "s3.port", uri_get_port (app->uri));
    conf_set_boolean (app->conf, "s3.ssl", uri_is_https (app->uri));
    
    return TRUE;
}
/*}}}*/

/*{{{ signal handlers */

#ifdef __APPLE__

typedef ucontext_t sig_ucontext_t;

#else /* !__APPLE__ */

/* This structure mirrors the one found in /usr/include/asm/ucontext.h */
typedef struct _sig_ucontext {
    unsigned long     uc_flags;
    struct ucontext   *uc_link;
    stack_t           uc_stack;
    struct sigcontext uc_mcontext;
    sigset_t          uc_sigmask;
} sig_ucontext_t;

#endif /* !__APPLE__ */

static void sigsegv_cb (int sig_num, siginfo_t *info, void * ucontext)
{
    void *array[50];
    void *caller_address;
    char **messages;
    int size, i;
    sig_ucontext_t *uc;
    FILE *f;
    
    g_fprintf (stderr, "Got segmentation fault !\n");

    uc = (sig_ucontext_t *)ucontext;

    /* Get the address at the time the signal was raised from the EIP (x86) */
#if __APPLE__
#ifdef __i368__
    caller_address = (void *) uc->uc_mcontext->__ss.__eip;
#else
    caller_address = (void *) uc->uc_mcontext->__ss.__rip;
#endif
#else /* !__APPLE__ */
#ifdef __i386__
    caller_address = (void *) uc->uc_mcontext.eip;
#else
    caller_address = (void *) uc->uc_mcontext.rip;
#endif
#endif /* !__APPLE__ */

    f = stderr;

    fprintf (f, "signal %d (%s), address is %p from %p\n", sig_num, strsignal (sig_num), info->si_addr, (void *)caller_address);

    size = backtrace (array, 50);

    /* overwrite sigaction with caller's address */
    array[1] = caller_address;

    messages = backtrace_symbols (array, size);

    /* skip first stack frame (points here) */
    for (i = 1; i < size && messages != NULL; ++i) {
        fprintf (f, "[bt]: (%d) %s\n", i, messages[i]);
    }

    fflush (f);

    free (messages);

    LOG_err (APP_LOG, "signal %d (%s), address is %p from %p\n", sig_num, strsignal (sig_num), info->si_addr, (void *)caller_address);

    // try to unmount FUSE mountpoint
    if (_app && _app->rfuse)
        rfuse_destroy (_app->rfuse);
}

// ignore SIGPIPE
static void sigpipe_cb (G_GNUC_UNUSED evutil_socket_t sig, G_GNUC_UNUSED short events, G_GNUC_UNUSED void *user_data)
{
    LOG_msg (APP_LOG, "Got SIGPIPE");
}

// USR1 signal: re-read configuration file
static void sigusr1_cb (G_GNUC_UNUSED evutil_socket_t sig, G_GNUC_UNUSED short events, G_GNUC_UNUSED void *user_data)
{
    ConfData *conf_new = conf_create();
    ConfData *conf_old = _app->conf;
    LOG_err (APP_LOG, "Got SIGUSR1");

    if (!conf_parse_file (conf_new, _app->conf_path)) {
        LOG_err (APP_LOG, "Failed to parse configuration file: %s", _app->conf_path);
        conf_destroy(conf_new);
    } else {
        const gchar *copy_entries[] = {"s3.host", "s3.port", "s3.versioning", "s3.access_key_id", "s3.secret_access_key", "s3.bucket_name", NULL};
        int i;

        _app->conf = conf_new;

        for (i = 0; copy_entries[i]; i++) {
            conf_copy_entry (_app->conf, conf_old, copy_entries[i], FALSE);
        }

        conf_destroy (conf_old);

        log_level = conf_get_int(_app->conf, "log.level");
    }
}

// USR2 signal: reopen log file
static void sigusr2_cb (G_GNUC_UNUSED evutil_socket_t sig, G_GNUC_UNUSED short events, G_GNUC_UNUSED void *user_data)
{
    Application *app = _app;

     // just flush, if log file name is not specified
    if (!app->log_file_name || !app->f_log) {
        fflush (app->f_log);
        return;
    }

    LOG_msg (APP_LOG, "Reopening log file: %s !", app->log_file_name);
    
    fflush (app->f_log);
    fclose (app->f_log);

    app->f_log = fopen (app->log_file_name, "a+");
    if (!app->f_log) {
        LOG_err (APP_LOG, "Failed to open log file: %s, output goes to stdout. Error: %s", app->log_file_name, strerror (errno));
        // XXX: set output to stdout
        logger_set_file (stdout);
        return;
    }
    
    logger_set_file (app->f_log);

}

// terminate application, freeing all used memory
static void sigint_cb (G_GNUC_UNUSED evutil_socket_t sig, G_GNUC_UNUSED short events, void *user_data)
{
    Application *app = (Application *) user_data;

    LOG_err (APP_LOG, "Got SIGINT");
    
    // terminate after running all active events
    application_exit (app);
}
// same as SIGINT
static void sigterm_cb (G_GNUC_UNUSED evutil_socket_t sig, G_GNUC_UNUSED short events, void *user_data)
{
    Application *app = (Application *) user_data;

    LOG_err (APP_LOG, "Got SIGTERM");

    // terminate after running all active events 
    application_exit (app);
}

/*}}}*/

/*{{{ application_finish_initialization_and_run */
static gint application_finish_initialization_and_run (Application *app)
{
    struct sigaction sigact;

/*{{{ create Pools */
    // create ClientPool for reading operations
    app->read_client_pool = client_pool_create (app, conf_get_int (app->conf, "pool.readers"),
        http_connection_create,
        http_connection_destroy,
        http_connection_set_on_released_cb,
        http_connection_check_rediness,
        http_connection_get_stats_info_caption,
        http_connection_get_stats_info_data
        );
    if (!app->read_client_pool) {
        LOG_err (APP_LOG, "Failed to create ClientPool !");
        application_exit (app);
        return -1;
    }

    // create ClientPool for writing operations
    app->write_client_pool = client_pool_create (app, conf_get_int (app->conf, "pool.writers"),
        http_connection_create,
        http_connection_destroy,
        http_connection_set_on_released_cb,
        http_connection_check_rediness,
        http_connection_get_stats_info_caption,
        http_connection_get_stats_info_data
        );
    if (!app->write_client_pool) {
        LOG_err (APP_LOG, "Failed to create ClientPool !");
        application_exit (app);
        return -1;
    }

    // create ClientPool for various operations
    app->ops_client_pool = client_pool_create (app, conf_get_int (app->conf, "pool.operations"),
        http_connection_create,
        http_connection_destroy,
        http_connection_set_on_released_cb,
        http_connection_check_rediness,
        http_connection_get_stats_info_caption,
        http_connection_get_stats_info_data
        );
    if (!app->ops_client_pool) {
        LOG_err (APP_LOG, "Failed to create ClientPool !");
        application_exit (app);
        return -1;
    }
/*}}}*/

/*{{{ CacheMng */
    app->cmng = cache_mng_create (app);
    if (!app->cmng) {
        LOG_err (APP_LOG, "Failed to create CacheMng !");
        application_exit (app);
        return -1;
    }
/*}}}*/

/*{{{ DirTree*/
    app->dir_tree = dir_tree_create (app);
    if (!app->dir_tree) {
        LOG_err (APP_LOG, "Failed to create DirTree !");
        application_exit (app);
        return -1;
    }
/*}}}*/

/*{{{ FUSE*/
    app->rfuse = rfuse_new (app, conf_get_string (app->conf, "app.mountpoint"), app->fuse_opts);
    if (!app->rfuse) {
        LOG_err (APP_LOG, "Failed to create FUSE fs ! Mount point: %s", conf_get_string (app->conf, "app.mountpoint"));
        application_exit (app);
        return -1;
    }
/*}}}*/

  // set global App variable
    _app = app;

/*{{{ signal handlers*/
    // SIGINT
    app->sigint_ev = evsignal_new (app->evbase, SIGINT, sigint_cb, app);
    event_add (app->sigint_ev, NULL);
    // SIGSEGV
    sigact.sa_sigaction = sigsegv_cb;
    sigact.sa_flags = (int)SA_RESETHAND | SA_SIGINFO;
    sigemptyset (&sigact.sa_mask);
    if (sigaction (SIGSEGV, &sigact, (struct sigaction *) NULL) != 0) {
        LOG_err (APP_LOG, "error setting signal handler for %d (%s)\n", SIGSEGV, strsignal(SIGSEGV));
        application_exit (app);
        return 1;
    }
    // SIGTERM
    app->sigterm_ev = evsignal_new (app->evbase, SIGTERM, sigterm_cb, app);
    event_add (app->sigterm_ev, NULL);
    // SIGABRT
    sigact.sa_sigaction = sigsegv_cb;
    sigact.sa_flags = (int)SA_RESETHAND | SA_SIGINFO;
    sigemptyset (&sigact.sa_mask);
    if (sigaction (SIGABRT, &sigact, (struct sigaction *) NULL) != 0) {
        LOG_err (APP_LOG, "error setting signal handler for %d (%s)\n", SIGABRT, strsignal(SIGABRT));
        application_exit (app);
        return 1;
    }
    // SIGPIPE
    app->sigpipe_ev = evsignal_new (app->evbase, SIGPIPE, sigpipe_cb, app);
    event_add (app->sigpipe_ev, NULL);

    // SIGUSR1
    app->sigusr1_ev = evsignal_new (app->evbase, SIGUSR1, sigusr1_cb, app);
    event_add (app->sigusr1_ev, NULL);

    // SIGUSR2
    app->sigusr2_ev = evsignal_new (app->evbase, SIGUSR2, sigusr2_cb, app);
    event_add (app->sigusr2_ev, NULL);

/*}}}*/
    
    if (!conf_get_boolean (app->conf, "app.foreground"))
        fuse_daemonize (0);

    return 0;
}
/*}}}*/

/*{{{ application_on_bucket_versioning_cb */
//  replies on bucket versioning information
static void application_on_bucket_versioning_cb (gpointer ctx, gboolean success,
    const gchar *buf, size_t buf_len)
{
    Application *app = (Application *)ctx;
    gchar *tmp;
    
    if (!success) {
        LOG_err (APP_LOG, "Failed to get bucket versioning!");
        application_exit (app);
        return;
    }

    if (buf_len > 1) {
        tmp = (gchar *)buf;
        tmp[buf_len - 1] = '\0';

        if (strstr (buf, "<Status>Enabled</Status>")) {
            LOG_debug (APP_LOG, "Bucket has versioning enabled !");
            conf_set_boolean (app->conf, "s3.versioning", TRUE);
        } else {
            LOG_debug (APP_LOG, "Bucket has versioning disabled !");
            conf_set_boolean (app->conf, "s3.versioning", FALSE);
        }
    } else {
        conf_set_boolean (app->conf, "s3.versioning", FALSE);
    }
    
    application_finish_initialization_and_run (app);
}
/*}}}*/

/*{{{ application_on_bucket_acl_cb */
//  replies on bucket ACL
static void application_on_bucket_acl_cb (gpointer ctx, gboolean success,
    G_GNUC_UNUSED const gchar *buf, G_GNUC_UNUSED size_t buf_len)
{
    Application *app = (Application *)ctx;
    
    if (!success) {
        LOG_err (APP_LOG, "Failed to get bucket ACL! Most likely you provided wrong AWS keys or bucket name !");
        application_exit (app);
        return;
    }
    
    // XXX: check ACL permissions
    
    bucket_client_get (app->service_con, "/?versioning", application_on_bucket_versioning_cb, app);
}
/*}}}*/

/*{{{ application_destroy */
static void application_destroy (Application *app)
{
    LOG_debug (APP_LOG, "Destroying application !");

    g_free (app->conf_path);
    if (app->read_client_pool)
        client_pool_destroy (app->read_client_pool);
    if (app->write_client_pool)
        client_pool_destroy (app->write_client_pool);
    if (app->ops_client_pool)
        client_pool_destroy (app->ops_client_pool);

    if (app->dir_tree)
        dir_tree_destroy (app->dir_tree);

    if (app->cmng)
        cache_mng_destroy (app->cmng);

    if (app->sigint_ev)
        event_free (app->sigint_ev);
    if (app->sigterm_ev)
        event_free (app->sigterm_ev);
    if (app->sigpipe_ev)
        event_free (app->sigpipe_ev);
    if (app->sigusr1_ev)
        event_free (app->sigusr1_ev);
     if (app->sigusr2_ev)
        event_free (app->sigusr2_ev);
   
    if (app->service_con)
        http_connection_destroy (app->service_con);

    if (app->stat_srv)
        stat_srv_destroy (app->stat_srv);

    // destroy Fuse
    if (app->rfuse)
        rfuse_destroy (app->rfuse);

    if (app->dns_base)
        evdns_base_free (app->dns_base, 0);
    if (app->evbase)
        event_base_free (app->evbase);

    if (app->uri)
        evhttp_uri_free (app->uri);

    if (app->conf)
        conf_destroy (app->conf);

    if (app->fuse_opts)
        g_free (app->fuse_opts);
    
#ifdef SSL_ENABLED
    SSL_CTX_free (app->ssl_ctx);
#endif

    logger_destroy ();

    if (app->f_log)
        fclose (app->f_log);
    
    if (app->log_file_name)
        g_free (app->log_file_name);

    g_free (app);
    
    ENGINE_cleanup ();
    CRYPTO_cleanup_all_ex_data ();
    ERR_free_strings ();
    ERR_remove_thread_state (NULL);
    EVP_cleanup ();
    CRYPTO_mem_leaks_fp (stderr);
}
/*}}}*/

/*{{{ main */
int main (int argc, char *argv[])
{
    Application *app;
    gboolean verbose = FALSE;
    gboolean version = FALSE;
    GError *error = NULL;
    GOptionContext *context;
    gchar **s_params = NULL;
    gchar **s_config = NULL;
    gboolean foreground = FALSE;
    gchar conf_str[1023];
    struct stat st;
    gboolean path_style = FALSE;
    gchar **cache_dir = NULL;
    gchar **s_fuse_opts = NULL;
    gchar **s_log_file = NULL;
    guint32 part_size = 0;
    gboolean disable_syslog = FALSE;
    gboolean disable_stats = FALSE;

    struct event_config *ev_config;
    
    app = g_new0 (Application, 1);
    app->conf_path = g_build_filename (SYSCONFDIR, "riofs.conf.xml", NULL);
    g_snprintf (conf_str, sizeof (conf_str), "Path to configuration file. Default: %s", app->conf_path);

    GOptionEntry entries[] = {
        { G_OPTION_REMAINING, 0, 0, G_OPTION_ARG_STRING_ARRAY, &s_params, NULL, NULL },
        { "config", 'c', 0, G_OPTION_ARG_FILENAME_ARRAY, &s_config, conf_str, NULL},
        { "foreground", 'f', 0, G_OPTION_ARG_NONE, &foreground, "Flag. Do not daemonize process.", NULL },
        { "cache-dir", 0, 0, G_OPTION_ARG_STRING_ARRAY, &cache_dir, "Set cache directory.", NULL },
        { "fuse-options", 'o', 0, G_OPTION_ARG_STRING_ARRAY, &s_fuse_opts, "Fuse options.", "\"opt[,opt...]\"" },
        { "path-style", 'p', 0, G_OPTION_ARG_NONE, &path_style, "Flag. Use legacy path-style access syntax.", NULL },
        { "disable-syslog", 0, 0, G_OPTION_ARG_NONE, &disable_syslog, "Flag. Disable logging to syslog.", NULL },
        { "disable-stats", 0, 0, G_OPTION_ARG_NONE, &disable_stats, "Flag. Disable Statistics HTTP interface.", NULL },
        { "part-size", 0, 0, G_OPTION_ARG_INT, &part_size, "Set file part size (in bytes).", NULL },
        { "log-file", 'l', 0, G_OPTION_ARG_STRING_ARRAY, &s_log_file, "File to write output.", NULL },
        { "verbose", 'v', 0, G_OPTION_ARG_NONE, &verbose, "Verbose output.", NULL },
        { "version", 0, 0, G_OPTION_ARG_NONE, &version, "Show application version and exit.", NULL },
        { NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL }
    };

    // init libraries
    CRYPTO_set_mem_functions (g_malloc0, g_realloc, g_free);
    ENGINE_load_builtin_engines ();
    ENGINE_register_all_complete ();
    ERR_load_crypto_strings ();
    OpenSSL_add_all_algorithms ();
#ifdef SSL_ENABLED
    SSL_load_error_strings ();
    SSL_library_init ();
#endif
    if (!RAND_poll ()) {
        fprintf(stderr, "RAND_poll() failed.\n");
        return -1;
    }
    g_random_set_seed (time (NULL));

    // init main app structure
    ev_config = event_config_new ();
    
#if __APPLE__
    // method select is the preferred method on OS X. kqueue and poll are not supported.
    event_config_avoid_method (ev_config, "kqueue");
    event_config_avoid_method (ev_config, "poll");
#endif
    
    app->evbase = event_base_new_with_config (ev_config);
    event_config_free (ev_config);
    
    if (!app->evbase) {
        LOG_err (APP_LOG, "Failed to create event base !");
        application_destroy (app);
        return -1;
    }

    app->dns_base = evdns_base_new (app->evbase, 1);
    if (!app->dns_base) {
        LOG_err (APP_LOG, "Failed to create DNS base !");
        application_destroy (app);
        return -1;
    }

    app->f_log = NULL;
    app->log_file_name = NULL;

/*{{{ cmd line args */

    // parse command line options
    context = g_option_context_new ("[http://s3.amazonaws.com] [bucketname] [mountpoint]");
    g_option_context_add_main_entries (context, entries, NULL);
    g_option_context_set_description (context, "Please set both AWSACCESSKEYID and AWSSECRETACCESSKEY environment variables!");
    if (!g_option_context_parse (context, &argc, &argv, &error)) {
        g_fprintf (stderr, "Failed to parse command line options: %s\n", error->message);
        application_destroy (app);
        g_option_context_free (context);
        return -1;
    }
    g_option_context_free (context);

    if (verbose)
        log_level = LOG_debug;
    else
        log_level = LOG_msg;

/*}}}*/

/*{{{ parse config file */

    // user provided alternative config path
    if (s_config && g_strv_length (s_config) > 0) {
        g_free (app->conf_path);
        app->conf_path = g_strdup (s_config[0]);
        g_strfreev (s_config);
    }
    
    app->conf = conf_create ();
    if (access (app->conf_path, R_OK) == 0) {
        LOG_debug (APP_LOG, "Using config file: %s", app->conf_path);
        
        if (!conf_parse_file (app->conf, app->conf_path)) {
            LOG_err (APP_LOG, "Failed to parse configuration file: %s", app->conf_path);
            application_destroy (app);
            return -1;
        }

    } else {
        LOG_debug (APP_LOG, "Configuration file does not exist, using predefined values.");
        conf_set_boolean (app->conf, "log.use_syslog", FALSE);
        conf_set_boolean (app->conf, "log.use_color", FALSE);
        conf_set_int (app->conf, "log.level", LOG_msg);
        
        conf_set_int (app->conf, "pool.writers", 2);
        conf_set_int (app->conf, "pool.readers", 2);
        conf_set_int (app->conf, "pool.operations", 4);
        conf_set_uint (app->conf, "pool.max_requests_per_pool", 100);

        conf_set_int (app->conf, "connection.timeout", -1);
        conf_set_int (app->conf, "connection.retries", -1);
        conf_set_int (app->conf, "connection.max_redirects", 20);

        conf_set_uint (app->conf, "s3.part_size", 1000);
        conf_set_uint (app->conf, "s3.keys_per_request", 5242880);

        conf_set_uint (app->conf, "filesystem.cache_dir_max_size", 1073741824);
        conf_set_uint (app->conf, "filesystem.dir_cache_max_time", 5);
        conf_set_boolean (app->conf, "filesystem.cache_enabled", TRUE);
        conf_set_string (app->conf, "filesystem.cache_dir", "/tmp/");
    }

    if (!conf_check_keys (app->conf, conf_keys_str, conf_keys_len)) {
        LOG_err (APP_LOG, "Configuration file is missing keys, please re-check your configuration file: %s", app->conf_path);
        application_destroy (app);
        return -1;
    }

    if (disable_syslog) {
        conf_set_boolean (app->conf, "log.use_syslog", FALSE);
    }
    // update logging settings
    logger_set_syslog (conf_get_boolean (app->conf, "log.use_syslog"));
    logger_set_color (conf_get_boolean (app->conf, "log.use_color"));

    if (cache_dir && g_strv_length (cache_dir) > 0) {
        conf_set_string (app->conf, "filesystem.cache_dir", cache_dir[0]);
        g_strfreev (cache_dir);
    }

    if (!verbose)
        log_level = conf_get_int (app->conf, "log.level");
/*}}}*/
    
    // check if --version is specified
    if (version) {
        g_fprintf (stdout, "RioFS File System v%s\n", VERSION);
        g_fprintf (stdout, "Copyright (C) 2012-2013 Paul Ionkin <paul.ionkin@gmail.com>\n");
        g_fprintf (stdout, "Copyright (C) 2012-2013 Skoobe GmbH. All rights reserved.\n");
        g_fprintf (stdout, "Libraries:\n");
        g_fprintf (stdout, " GLib: %d.%d.%d   libevent: %s  fuse: %d.%d",
                GLIB_MAJOR_VERSION, GLIB_MINOR_VERSION, GLIB_MICRO_VERSION,
                LIBEVENT_VERSION,
                FUSE_MAJOR_VERSION, FUSE_MINOR_VERSION
        );
#ifdef __APPLE__
        g_fprintf (stdout, "\n");
#else
        g_fprintf (stdout, "  glibc: %s\n", gnu_get_libc_version ());
#endif
        g_fprintf (stdout, "Features:\n");
        g_fprintf (stdout, " Cache enabled: %s\n", conf_get_boolean (app->conf, "filesystem.cache_enabled") ? "True" : "False");
        g_fprintf (stdout, " libevent backend method: %s\n", event_base_get_method(app->evbase));

        /*
        {
            int i;
            const char **methods = event_get_supported_methods ();

            g_fprintf (stdout, " Available libevent backend methods:\n");
            for (i = 0; methods[i] != NULL; ++i) {
                g_fprintf (stdout, "  %s\n", methods[i]);
            }
        }
        */

        return 0;
    }

    // try to get access parameters from the environment
    if (getenv ("AWSACCESSKEYID")) {
        conf_set_string (app->conf, "s3.access_key_id", getenv ("AWSACCESSKEYID"));
    // else check if it's set it the config file
    } else {
        if (!conf_node_exists (app->conf, "s3.access_key_id")) {
            LOG_err (APP_LOG, "Environment variables are not set!\nTry `%s --help' for more information.", argv[0]);
            application_destroy (app);
            return -1;
        }
    }
    if (getenv ("AWSSECRETACCESSKEY")) {
        conf_set_string (app->conf, "s3.secret_access_key", getenv ("AWSSECRETACCESSKEY"));
    } else {
        if (!conf_node_exists (app->conf, "s3.secret_access_key")) {
            LOG_err (APP_LOG, "Environment variables are not set!\nTry `%s --help' for more information.", argv[0]);
            application_destroy (app);
            return -1;
        }
    }

    // check if both strings are set
    if (!conf_get_string (app->conf, "s3.access_key_id") || !conf_get_string (app->conf, "s3.secret_access_key")) {
        LOG_err (APP_LOG, "Environment variables are not set!\nTry `%s --help' for more information.", argv[0]);
        application_destroy (app);
        return -1;
    }

    if (!s_params || g_strv_length (s_params) != 3) {
        LOG_err (APP_LOG, "Wrong number of provided arguments!\nTry `%s --help' for more information.", argv[0]);
        application_destroy (app);
        return -1;
    }

    // foreground is set
    if (foreground)
        conf_set_boolean (app->conf, "app.foreground", foreground);

    if (path_style)
        conf_set_boolean (app->conf, "s3.path_style", path_style);

    if (part_size)
        conf_set_uint (app->conf, "s3.part_size", part_size);

    if (disable_stats)
        conf_set_boolean (app->conf, "statistics.enabled", FALSE);

    conf_set_string (app->conf, "s3.bucket_name", s_params[1]);
    if (!application_set_url (app, s_params[0])) {
        application_destroy (app);
        return -1;
    }

    if (s_fuse_opts && g_strv_length (s_fuse_opts) > 0) {
        app->fuse_opts = g_strdup (s_fuse_opts[0]);
        g_strfreev (s_fuse_opts);
    }

    if (s_log_file  && g_strv_length (s_log_file) > 0) {
        app->log_file_name = g_strdup (s_log_file[0]);
        app->f_log = fopen (s_log_file[0], "a+");
        if (!app->f_log) {
            LOG_err (APP_LOG, "Failed to open log file: %s Error: %s", s_log_file[0], strerror (errno));
            application_destroy (app);
            return -1;
        }

        LOG_debug (APP_LOG, "Using %s for storing application logs.", s_log_file[0]);
        logger_set_file (app->f_log);
        g_strfreev (s_log_file);
    }

    conf_set_string (app->conf, "app.mountpoint", s_params[2]);

    // check if directory exists
    if (stat (conf_get_string (app->conf, "app.mountpoint"), &st) == -1) {
        LOG_err (APP_LOG, "Mountpoint %s does not exist! Please check directory permissions!", 
            conf_get_string (app->conf, "app.mountpoint"));
        application_destroy (app);
        return -1;
    }
    // check if it's a directory
    if (!S_ISDIR (st.st_mode)) {
        LOG_err (APP_LOG, "Mountpoint %s is not a directory!", conf_get_string (app->conf, "app.mountpoint"));
        application_destroy (app);
        return -1;
    }
    
    g_strfreev (s_params);

#ifdef SSL_ENABLED
    app->ssl_ctx = SSL_CTX_new (SSLv23_client_method ());
    if (!app->ssl_ctx) {
        LOG_err (APP_LOG, "Failed to initialize SSL engine !");
        application_exit (app);
        return -1;
    }
    SSL_CTX_set_options (app->ssl_ctx, SSL_OP_ALL);

#endif
    app->stat_srv = stat_srv_create (app);
    if (!app->stat_srv) {
        application_exit (app);
        return -1;
    }
  
    // perform the initial request to get  bucket ACL (handles redirect as well)
    app->service_con = http_connection_create (app);
    if (!app->service_con)  {
        application_destroy (app);
        return -1;
    }
    bucket_client_get (app->service_con, "/?acl", application_on_bucket_acl_cb, app);

    // start the loop
    event_base_dispatch (app->evbase);

    application_destroy (app);

    return 0;
}
/*}}}*/
