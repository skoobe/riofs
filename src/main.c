#include "include/global.h"
#include "include/s3http_connection.h"
#include "include/dir_tree.h"
#include "include/s3fuse.h"
#include "include/s3http_client_pool.h"

#define APP_LOG "main"

struct _Application {
    struct event_base *evbase;
    struct evdns_base *dns_base;
    
    S3Fuse *s3fuse;
    DirTree *dir_tree;
    S3HttpClientPool *s3http_client_pool;
    S3HttpConnection *s3http_connection;

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

S3HttpConnection *application_get_s3http_connection (Application *app)
{
    return app->s3http_connection;
}

S3HttpConnection *application_get_s3http_client_pool (Application *app)
{
    return app->s3http_client_pool;
}


/*{{{ signal handlers */
/* This structure mirrors the one found in /usr/include/asm/ucontext.h */
typedef struct _sig_ucontext {
    unsigned long     uc_flags;
    struct ucontext   *uc_link;
    stack_t           uc_stack;
    struct sigcontext uc_mcontext;
    sigset_t          uc_sigmask;
} sig_ucontext_t;

static void sigsegv_cb (int sig_num, siginfo_t *info, void * ucontext)
{
    void *array[50];
    void *caller_address;
    char **messages;
    int size, i;
    sig_ucontext_t *uc;
    FILE *f;
    
    g_fprintf (stderr, "Got Sigfault !\n");

	uc = (sig_ucontext_t *)ucontext;

    /* Get the address at the time the signal was raised from the EIP (x86) */
#ifdef __i386__
    caller_address = (void *) uc->uc_mcontext.eip;   
#else
    caller_address = (void *) uc->uc_mcontext.rip;   
#endif

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
//	continue execution, default handler will create core file
//	exit (EXIT_FAILURE);
}

static void sigpipe_cb (G_GNUC_UNUSED evutil_socket_t sig, G_GNUC_UNUSED short events, G_GNUC_UNUSED void *user_data)
{
	LOG_msg (APP_LOG, "Got SIGPIPE");
}

// terminate application without calling destruction functions
static void sigusr1_cb (G_GNUC_UNUSED evutil_socket_t sig, G_GNUC_UNUSED short events, G_GNUC_UNUSED void *user_data)
{
	LOG_err (APP_LOG, "Got SIGUSR1");
    
    exit (1);
}

// terminate application, freeing all used memory
static void sigint_cb (G_GNUC_UNUSED evutil_socket_t sig, G_GNUC_UNUSED short events, void *user_data)
{
	Application *app = (Application *) user_data;

	LOG_err (APP_LOG, "Got SIGINT");

    // terminate after running all active events 
    event_base_loopexit (app->evbase, NULL);
}
/*}}}*/

int main (int argc, char *argv[])
{
    Application *app;
    struct event *sigint_ev = NULL;
    struct event *sigpipe_ev = NULL;
    struct event *sigusr1_ev = NULL;
    struct sigaction sigact;

    // init libraries
    ENGINE_load_builtin_engines ();
    ENGINE_register_all_complete ();

    // init main app structure
    app = g_new0 (Application, 1);
    app->evbase = event_base_new ();

    if (!app->evbase) {
        LOG_err (APP_LOG, "Failed to create event base !");
        return -1;
    }

    app->dns_base = evdns_base_new (app->evbase, 1);
    if (!app->dns_base) {
        LOG_err (APP_LOG, "Failed to create DNS base !");
        return -1;
    }

    // get access parameters from the enviorment
    // XXX: extend it
    app->aws_access_key_id = getenv("AWSACCESSKEYID");
    app->aws_secret_access_key = getenv("AWSSECRETACCESSKEY");

    if (!app->aws_access_key_id || !app->aws_secret_access_key) {
        LOG_err (APP_LOG, "Please set both AWSACCESSKEYID and AWSSECRETACCESSKEY environment variables !");
        return -1;
    }

    // XXX: parse command line
    if (argc < 3) {
        LOG_err (APP_LOG, "Please use s3ffs [http://s3.amazonaws.com] [bucketname] [FUSE params] [mountpoint]");
        return -1;
    }

    app->url = evhttp_uri_parse (argv[1]);
    if (!app->url) {
        LOG_err (APP_LOG, "Failed to parse URL, please use s3ffs [http://s3.amazonaws.com] [bucketname] [FUSE params] [mountpoint]");
        return -1;
    }
    app->s_url = g_strdup (argv[1]);
    app->bucket_name = g_strdup (argv[2]);
    
    // create S3HTTPClientPool
    app->s3http_client_pool = s3http_client_pool_create (app->evbase, app->dns_base, 10);
    if (!app->s3http_client_pool) {
        LOG_err (APP_LOG, "Failed to create S3HTTPClientPool !");
        return -1;
    }

    // create S3HttpConnection
    app->s3http_connection = s3http_connection_create (app, app->url, app->bucket_name);
    if (!app->s3http_connection) {
        LOG_err (APP_LOG, "Failed to create S3HttpConnection !");
        return -1;
    }

    // create DirTree
    app->dir_tree = dir_tree_create (app);
    if (!app->dir_tree) {
        LOG_err (APP_LOG, "Failed to create DirTree !");
        return -1;
    }

    // create FUSE
    argv += 2;
    argc -= 2;
    app->s3fuse = s3fuse_new (app, argc, argv);
    if (!app->s3fuse) {
        LOG_err (APP_LOG, "Failed to create FUSE fs !");
        return -1;
    }

	// install signal handlers
	// SIGINT
	sigint_ev = evsignal_new (app->evbase, SIGINT, sigint_cb, app);
	event_add (sigint_ev, NULL);
	// SIGSEGV
    sigact.sa_sigaction = sigsegv_cb;
    sigact.sa_flags = (int)SA_RESETHAND | SA_SIGINFO;
	sigemptyset (&sigact.sa_mask);
    if (sigaction (SIGSEGV, &sigact, (struct sigaction *) NULL) != 0) {
        fprintf (stderr, "error setting signal handler for %d (%s)\n", SIGSEGV, strsignal(SIGSEGV));
		return 1;
    }
	// SIGABRT
    sigact.sa_sigaction = sigsegv_cb;
    sigact.sa_flags = (int)SA_RESETHAND | SA_SIGINFO;
	sigemptyset (&sigact.sa_mask);
    if (sigaction (SIGABRT, &sigact, (struct sigaction *) NULL) != 0) {
        fprintf (stderr, "error setting signal handler for %d (%s)\n", SIGABRT, strsignal(SIGABRT));
		return 1;
    }
	// SIGPIPE
	sigpipe_ev = evsignal_new (app->evbase, SIGPIPE, sigpipe_cb, app);
	event_add (sigpipe_ev, NULL);
    // SIGUSR1
	sigusr1_ev = evsignal_new (app->evbase, SIGUSR1, sigusr1_cb, app);
	event_add (sigusr1_ev, NULL);


    // start the loop
    event_base_dispatch (app->evbase);
    
    // destroy S3Fuse 
    s3fuse_destroy (app->s3fuse);

    return 0;
}
