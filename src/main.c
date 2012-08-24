#include "include/global.h"
#include "include/http_proto.h"

struct _Application {
    struct event_base *evbase;
    struct evdns_base *dns_base;
    char *mountpoint;
    int multithreaded;
    int foreground;
    // the session that we use to process the fuse stuff
    struct fuse_session *session;

    struct fuse_chan *chan;
    // the event that we use to receive requests
    struct event *ev;
    
    // what our receive-message length is
    size_t recv_size;

    // the buffer that we use to receive events
    char *recv_buf;
};

// prints a message string to stdout
static void logger_log_msg (G_GNUC_UNUSED const gchar *file, G_GNUC_UNUSED gint line, G_GNUC_UNUSED const gchar *func, 
        const gchar *format, ...)
{
    va_list args;
    char out_str[1024];

    va_start (args, format);
        g_vsnprintf (out_str, 1024, format, args);
    va_end (args);

#ifdef DEBUG
    g_fprintf (stdout, "%s:%d(%s) %s\n", file, line, func, out_str);
#else
    g_fprintf (stdout, "%s\n",out_str);
#endif
}

#define LOG_msg(x...) \
G_STMT_START { \
    logger_log_msg (__FILE__, __LINE__, __func__, x); \
} G_STMT_END

#define LOG_err(x...) \
G_STMT_START { \
    logger_log_msg (__FILE__, __LINE__, __func__, x); \
} G_STMT_END

void hello_init (void *userdata, struct fuse_conn_info *conn) {
    LOG_msg ("[hello.init] userdata=%p, conn=%p", userdata, conn);
}

void hello_destroy (void *userdata) {
    LOG_msg ("[hello.destroy] userdata=%p", userdata);
}

/*
void hello_readdir (fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
    int err = 0;
    struct dirbuf buf;

    ("[hello.readdir] ino=%lu, size=%zu, off=%zu, fi=%p", ino, size, off, fi);

    // there exists only one dir
    if (ino != 1) {
        fuse_reply_err(req, ENOTDIR);
        return;
    }

    // fill in the dirbuf
    if (dirbuf_init(&buf, size, off))
        ERROR("failed to init dirbuf");

    err =   dirbuf_add(req, &buf, 0, 1,  ".",        1,    S_IFDIR )
        ||  dirbuf_add(req, &buf, 1, 2,  "..",       1,    S_IFDIR )
        ||  dirbuf_add(req, &buf, 2, 3,  file_name,  2,    S_IFREG );

    if (err < 0)
        ERROR("failed to add dirents to buf");
    
    // send it
    if ((err = -dirbuf_done(req, &buf)))
        EERROR(-err, "failed to send buf");

    // success
    return;

error:
    if ((err = fuse_reply_err(req, err ? err : EIO)))
        EWARNING(err, "failed to send error reply");
}
*/

struct fuse_lowlevel_ops hello_llops = {
    .init = &hello_init,
    .destroy = &hello_destroy,

    /*
    .lookup = &hello_lookup,
    .getattr = &hello_getattr,

    .open = &hello_open,

    .read = &hello_read,


    .getxattr = hello_getxattr,
    .readdir = &hello_readdir,
   */
};

static void _evfuse_ev_read (evutil_socket_t fd, short what, void *arg) {
    Application *app = (Application *) arg;
    struct fuse_chan *ch = app->chan;
    int res;

    if (!ch) {
        LOG_err ("OPS");
        return;
    }
    
    LOG_msg ("in func: %d", fuse_chan_bufsize (ch));


    // loop until we complete a recv
    do {
        // a new fuse_req is available
        res = fuse_chan_recv(&ch, app->recv_buf, app->recv_size);
    } while (res == -EINTR);

    if (res == 0)
        LOG_err("fuse_chan_recv gave EOF");

    if (res < 0 && res != -EAGAIN)
        LOG_err("fuse_chan_recv failed: %s", strerror(-res));
    
    if (res > 0) {
        LOG_msg("got %d bytes from /dev/fuse", res);

        // received a fuse_req, so process it
        fuse_session_process(app->session, app->recv_buf, res, ch);
    }
    
    // reschedule
    if (event_add(app->ev, NULL))
        LOG_err("event_add");

    // ok, wait for the next event
    return;

error:
    // close, but don't free
    return;
 //   evfuse_close(ctx);
}

int main (int argc, char *argv[])
{
    Application *app;
    struct fuse_args fuse_args = FUSE_ARGS_INIT(argc, argv);
    struct fuse_lowlevel_ops llops;
    HTTPConnection *con;

    app = g_new0 (Application, 1);
    app->evbase = event_base_new ();

    if (!app->evbase) {
        LOG_err ("Failed to create evbase !");
        return -1;
    }

    app->dns_base = evdns_base_new (app->evbase, 0);
    if (!app->dns_base) {
        LOG_err ("Failed to create dns_base !");
        return -1;
    }

    con = http_connection_new (app, argv[1]);

    /*
    llops = hello_llops;
    if (fuse_parse_cmdline (&fuse_args, &app->mountpoint, &app->multithreaded, &app->foreground) == -1) {
        LOG_err ("fuse_parse_cmdline");
        return -1;
    }

    if ((app->chan = fuse_mount (app->mountpoint, &fuse_args)) == NULL) {
        LOG_err ("fuse_mount_common");
        return -1;
    }

    // the receive buffer stufff
    app->recv_size = fuse_chan_bufsize (app->chan);
    LOG_msg ("in main: %d", app->recv_size);

    // allocate the recv buffer
    if ((app->recv_buf = malloc (app->recv_size)) == NULL) {
        LOG_err ("failed to malloc memory !");
        return -1;
    }
    
    // allocate a low-level session
    if ((app->session = fuse_lowlevel_new (&fuse_args, &llops, sizeof (llops), app)) == NULL) {
        LOG_err ("fuse_lowlevel_new");
        return -1;
    }

    fuse_session_add_chan (app->session, app->chan);

    if ((app->ev = event_new (app->evbase, fuse_chan_fd (app->chan), EV_READ, &_evfuse_ev_read, app)) == NULL) {
        LOG_err ("event_new");
        return -1;
    }

    if (event_add (app->ev, NULL)) {
        LOG_err ("event_add");
        return -1;
    }
*/
    event_base_dispatch (app->evbase);

    return 0;
}
