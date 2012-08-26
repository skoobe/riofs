#include "include/fuse.h"

struct _FuseInfo {
    Application *app;
    DirTree *dir_tree;
    
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

static void fuse_ll_readdir (fuse_req_t req, fuse_ino_t ino, 
    size_t size, off_t off, struct fuse_file_info *fi);
static void fuse_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name);

static struct fuse_lowlevel_ops ll_oper = {
	.readdir	= fuse_ll_readdir,
	.lookup		= fuse_ll_lookup,
    /*
	.getattr	= hello_ll_getattr,
	.open		= hello_ll_open,
	.read		= hello_ll_read,
   */
};

static void fuse_on_read (evutil_socket_t fd, short what, void *arg);

FuseInfo *fuse_create (Application *app, int argc, char *argv[])
{
    FuseInfo *fusei;
    struct fuse_args fuse_args = FUSE_ARGS_INIT(argc, argv);
    char *mountpoint;
    int multithreaded;
    int foreground;

    fusei = g_new0 (FuseInfo, 1);
    fusei->app = app;
    fusei->dir_tree = application_get_dir_tree (app);
    
    if (fuse_parse_cmdline (&fuse_args, &mountpoint, &multithreaded, &foreground) == -1) {
        LOG_err ("fuse_parse_cmdline");
        return NULL;
    }

    if ((fusei->chan = fuse_mount (mountpoint, &fuse_args)) == NULL) {
        LOG_err ("fuse_mount_common");
        return NULL;
    }

    // the receive buffer stufff
    fusei->recv_size = fuse_chan_bufsize (fusei->chan);

    // allocate the recv buffer
    if ((fusei->recv_buf = g_malloc (fusei->recv_size)) == NULL) {
        LOG_err ("failed to malloc memory !");
        return NULL;
    }
    
    // allocate a low-level session
    if ((fusei->session = fuse_lowlevel_new (&fuse_args, &ll_oper, sizeof (ll_oper), fusei)) == NULL) {
        LOG_err ("fuse_lowlevel_new");
        return NULL;
    }
    
    /*
    if (fuse_set_signal_handlers (fusei->session) == -1) {
        LOG_err ("fuse_set_signal_handlers");
        return NULL;
    }
    */

    fuse_session_add_chan (fusei->session, fusei->chan);

    if ((fusei->ev = event_new (application_get_evbase (app), fuse_chan_fd (fusei->chan), EV_READ, &fuse_on_read, fusei)) == NULL) {
        LOG_err ("event_new");
        return NULL;
    }

    if (event_add (fusei->ev, NULL)) {
        LOG_err ("event_add");
        return NULL;
    }

    return fusei;
}

static void fuse_on_read (evutil_socket_t fd, short what, void *arg)
{
    FuseInfo *fusei = (FuseInfo *)arg;
    struct fuse_chan *ch = fusei->chan;
    int res;

    if (!ch) {
        LOG_err ("OPS");
        return;
    }
    
    // loop until we complete a recv
    do {
        // a new fuse_req is available
        res = fuse_chan_recv (&ch, fusei->recv_buf, fusei->recv_size);
    } while (res == -EINTR);

    if (res == 0)
        LOG_err("fuse_chan_recv gave EOF");

    if (res < 0 && res != -EAGAIN)
        LOG_err("fuse_chan_recv failed: %s", strerror(-res));
    
    if (res > 0) {
        LOG_msg("got %d bytes from /dev/fuse", res);

        // received a fuse_req, so process it
        fuse_session_process (fusei->session, fusei->recv_buf, res, ch);
    }
    
    // reschedule
    if (event_add (fusei->ev, NULL))
        LOG_err("event_add");

    // ok, wait for the next event
    return;

}

struct dirbuf {
	char *p;
	size_t size;
};

static void dirbuf_add(fuse_req_t req, struct dirbuf *b, const char *name,
		       fuse_ino_t ino)
{
	struct stat stbuf;
	size_t oldsize = b->size;
	b->size += fuse_add_direntry(req, NULL, 0, name, NULL, 0);
	b->p = (char *) realloc(b->p, b->size);
	memset(&stbuf, 0, sizeof(stbuf));
	stbuf.st_ino = ino;
	fuse_add_direntry(req, b->p + oldsize, b->size - oldsize, name, &stbuf,
			  b->size);
}


static void fuse_on_readdir (fuse_req_t req, int result, const char *buf, size_t size)
{
    if (!result) {
		fuse_reply_err(req, ENOTDIR);
        return;
    }
    
    return fuse_reply_buf (req, buf, size);
}

static void fuse_ll_readdir (fuse_req_t req, fuse_ino_t ino, 
    size_t size, off_t off, struct fuse_file_info *fi)
{
    FuseInfo *fusei;
    gboolean res;

    LOG_debug ("readdir  inode: %d, size: %zd, off: %d", ino, size, off);
    fusei = fuse_req_userdata (req);

    // fill directory buffer for "ino" directory
    res = dir_tree_fill_dir_buf (fusei->dir_tree, ino, size, off, fuse_on_readdir, req);

    if (!res)
        fuse_reply_err(req, ENOTDIR);

/*
	if (ino != 1)
		fuse_reply_err(req, ENOTDIR);
	else {
        struct dirbuf b;

		memset(&b, 0, sizeof(b));
		dirbuf_add(req, &b, ".", 1);
		dirbuf_add(req, &b, "..", 1);
		dirbuf_add(req, &b, "TEST", 2);
		reply_buf_limited(req, b.p, b.size, off, size);
		free(b.p);
	}
*/
}

static int hello_stat(fuse_ino_t ino, struct stat *stbuf)
{
	stbuf->st_ino = ino;
	switch (ino) {
	case 1:
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 2;
		break;

	case 2:
		stbuf->st_mode = S_IFREG | 0444;
		stbuf->st_nlink = 1;
		stbuf->st_size = strlen("opa");
		break;

	default:
		return -1;
	}
	return 0;
}

static void fuse_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	struct fuse_entry_param e;

	if (parent != 1 || strcmp(name, "TEST") != 0)
		fuse_reply_err(req, ENOENT);
	else {
		memset(&e, 0, sizeof(e));
		e.ino = 2;
		e.attr_timeout = 1.0;
		e.entry_timeout = 1.0;
		hello_stat(e.ino, &e.attr);

		fuse_reply_entry(req, &e);
	}
}

#if 0
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

#endif
