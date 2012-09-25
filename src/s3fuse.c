#include "include/s3fuse.h"
#include "include/dir_tree.h"

/*{{{ struct */
struct _S3Fuse {
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

#define FUSE_LOG "fuse"

static void s3fuse_on_read (evutil_socket_t fd, short what, void *arg);
static void s3fuse_readdir (fuse_req_t req, fuse_ino_t ino, 
    size_t size, off_t off, struct fuse_file_info *fi);
static void s3fuse_lookup (fuse_req_t req, fuse_ino_t parent, const char *name);
static void s3fuse_getattr (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
static void s3fuse_setattr (fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi);
static void s3fuse_open (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
static void s3fuse_release (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
static void s3fuse_read (fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
static void s3fuse_write (fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi);
static void s3fuse_create (fuse_req_t req, fuse_ino_t parent_inode, const char *name, mode_t mode, struct fuse_file_info *fi);

static struct fuse_lowlevel_ops s3fuse_opers = {
	.readdir	= s3fuse_readdir,
	.lookup		= s3fuse_lookup,
    .getattr	= s3fuse_getattr,
    .setattr	= s3fuse_setattr,
	.open		= s3fuse_open,
	.release	= s3fuse_release,
	.read		= s3fuse_read,
	.write		= s3fuse_write,
	.create		= s3fuse_create,
};
/*}}}*/

/*{{{ create / destroy */

// create S3Fuse object
// create fuse handle and add it to libevent polling
S3Fuse *s3fuse_new (Application *app, int argc, char *argv[])
{
    S3Fuse *s3fuse;
    struct fuse_args fuse_args = FUSE_ARGS_INIT(argc, argv);

    s3fuse = g_new0 (S3Fuse, 1);
    s3fuse->app = app;
    s3fuse->dir_tree = application_get_dir_tree (app);
    
    if (fuse_parse_cmdline (&fuse_args, &s3fuse->mountpoint, &s3fuse->multithreaded, &s3fuse->foreground) == -1) {
        LOG_err (FUSE_LOG, "fuse_parse_cmdline");
        return NULL;
    }

    LOG_debug (FUSE_LOG, "FUSE params: mountpoint: %s, multithreaded: %d, foreground: %d", 
        s3fuse->mountpoint, s3fuse->multithreaded, s3fuse->foreground);

    if ((s3fuse->chan = fuse_mount (s3fuse->mountpoint, &fuse_args)) == NULL) {
        LOG_err (FUSE_LOG, "fuse_mount_common");
        return NULL;
    }

    // the receive buffer stuff
    s3fuse->recv_size = fuse_chan_bufsize (s3fuse->chan);

    // allocate the recv buffer
    if ((s3fuse->recv_buf = g_malloc (s3fuse->recv_size)) == NULL) {
        LOG_err (FUSE_LOG, "failed to malloc memory !");
        return NULL;
    }
    
    // allocate a low-level session
    s3fuse->session = fuse_lowlevel_new (&fuse_args, &s3fuse_opers, sizeof (s3fuse_opers), s3fuse);
    if (!s3fuse->session) {
        LOG_err (FUSE_LOG, "fuse_lowlevel_new");
        return NULL;
    }
    
    fuse_session_add_chan (s3fuse->session, s3fuse->chan);

    s3fuse->ev = event_new (application_get_evbase (app), 
        fuse_chan_fd (s3fuse->chan), EV_READ, &s3fuse_on_read, 
        s3fuse
    );
    if (!s3fuse->ev) {
        LOG_err (FUSE_LOG, "event_new");
        return NULL;
    }

    if (event_add (s3fuse->ev, NULL)) {
        LOG_err (FUSE_LOG, "event_add");
        return NULL;
    }

    return s3fuse;
}

void s3fuse_destroy (S3Fuse *s3fuse)
{
    fuse_unmount (s3fuse->mountpoint, s3fuse->chan);
    free (s3fuse->mountpoint);
    g_free (s3fuse->recv_buf);
    event_free (s3fuse->ev);
    fuse_session_destroy (s3fuse->session);
    g_free (s3fuse);
}

// low level fuse reading operations
static void s3fuse_on_read (evutil_socket_t fd, short what, void *arg)
{
    S3Fuse *s3fuse = (S3Fuse *)arg;
    struct fuse_chan *ch = s3fuse->chan;
    int res;

    if (!ch) {
        LOG_err (FUSE_LOG, "OPS");
        return;
    }
    
    // loop until we complete a recv
    do {
        // a new fuse_req is available
        res = fuse_chan_recv (&ch, s3fuse->recv_buf, s3fuse->recv_size);
    } while (res == -EINTR);

    if (res == 0)
        LOG_err (FUSE_LOG, "fuse_chan_recv gave EOF");

    if (res < 0 && res != -EAGAIN)
        LOG_err (FUSE_LOG, "fuse_chan_recv failed: %s", strerror(-res));
    
    if (res > 0) {
        LOG_debug (FUSE_LOG, "got %d bytes from /dev/fuse", res);

        fuse_session_process (s3fuse->session, s3fuse->recv_buf, res, ch);
    }
    
    // reschedule
    if (event_add (s3fuse->ev, NULL))
        LOG_err (FUSE_LOG, "event_add");

    // ok, wait for the next event
    return;
}
/*}}}*/

/*{{{ readdir operation */

#define min(x, y) ((x) < (y) ? (x) : (y))

// return newly allocated buffer which holds directory entry
void s3fuse_add_dirbuf (fuse_req_t req, struct dirbuf *b, const char *name, fuse_ino_t ino)
{
    struct stat stbuf;
    size_t oldsize = b->size;
    
    LOG_debug (FUSE_LOG, "add_dirbuf  ino: %d, name: %s", ino, name);

    // get required buff size
	b->size += fuse_add_direntry (req, NULL, 0, name, NULL, 0);

    // extend buffer
	b->p = (char *) g_realloc (b->p, b->size);
	memset (&stbuf, 0, sizeof (stbuf));
	stbuf.st_ino = ino;
    // add entry
	fuse_add_direntry (req, b->p + oldsize, b->size - oldsize, name, &stbuf, b->size);
}

// readdir callback
// Valid replies: fuse_reply_buf() fuse_reply_err()
static void s3fuse_readdir_cb (fuse_req_t req, gboolean success, size_t max_size, off_t off, const char *buf, size_t buf_size)
{
    LOG_debug (FUSE_LOG, "readdir_cb  success: %s, buf_size: %zd, size: %zd, off: %"OFF_FMT, success?"YES":"NO", buf_size, max_size, off);

    if (!success) {
		fuse_reply_err (req, ENOTDIR);
        return;
    }

	if (off < buf_size)
		fuse_reply_buf (req, buf + off, min (buf_size - off, max_size));
	else
	    fuse_reply_buf (req, NULL, 0);
}

// FUSE lowlevel operation: readdir
// Valid replies: fuse_reply_buf() fuse_reply_err()
static void s3fuse_readdir (fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi)
{
    S3Fuse *s3fuse = fuse_req_userdata (req);

    LOG_debug (FUSE_LOG, "readdir  inode: %"INO_FMT", size: %zd, off: %"OFF_FMT, ino, size, off);
    
    // fill directory buffer for "ino" directory
    dir_tree_fill_dir_buf (s3fuse->dir_tree, ino, size, off, s3fuse_readdir_cb, req);
}
/*}}}*/

/*{{{ getattr operation */

// getattr callback
static void s3fuse_getattr_cb (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size, time_t ctime)
{
    struct stat stbuf;

    LOG_debug (FUSE_LOG, "getattr_cb  success: %s", success?"YES":"NO");
    if (!success) {
		fuse_reply_err (req, ENOENT);
        return;
    }
    memset (&stbuf, 0, sizeof(stbuf));
    stbuf.st_ino = ino;
    stbuf.st_mode = mode;
	stbuf.st_nlink = 1;
	stbuf.st_size = file_size;
    stbuf.st_ctime = ctime;
    stbuf.st_atime = ctime;
    stbuf.st_mtime = ctime;
    
    fuse_reply_attr (req, &stbuf, 1.0);
}

// FUSE lowlevel operation: getattr
// Valid replies: fuse_reply_attr() fuse_reply_err()
static void s3fuse_getattr (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    S3Fuse *s3fuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, "getattr  for %d", ino);

    dir_tree_getattr (s3fuse->dir_tree, ino, s3fuse_getattr_cb, req);
}
/*}}}*/

/*{{{ setattr operation */
// setattr callback
static void s3fuse_setattr_cb (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size)
{
    struct stat stbuf;

    LOG_debug (FUSE_LOG, "setattr_cb  success: %s", success?"YES":"NO");
    if (!success) {
		fuse_reply_err (req, ENOENT);
        return;
    }
    memset (&stbuf, 0, sizeof(stbuf));
    stbuf.st_ino = ino;
    stbuf.st_mode = mode;
	stbuf.st_nlink = 1;
	stbuf.st_size = file_size;
    
    fuse_reply_attr (req, &stbuf, 1.0);
}

// FUSE lowlevel operation: setattr
// Valid replies: fuse_reply_attr() fuse_reply_err()
static void s3fuse_setattr (fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi)
{
    S3Fuse *s3fuse = fuse_req_userdata (req);

    dir_tree_setattr (s3fuse->dir_tree, ino, attr, to_set, s3fuse_setattr_cb, req, fi);
}
/*}}}*/

/*{{{ lookup operation*/

// lookup callback
static void s3fuse_lookup_cb (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size, time_t ctime)
{
	struct fuse_entry_param e;

    LOG_debug (FUSE_LOG, "lookup_cb  success: %s", success?"YES":"NO");
    if (!success) {
		fuse_reply_err (req, ENOENT);
        return;
    }

    memset(&e, 0, sizeof(e));
    e.ino = ino;
    e.attr_timeout = 1.0;
    e.entry_timeout = 1.0;

    e.attr.st_ino = ino;
    e.attr.st_mode = mode;
	e.attr.st_nlink = 1;
	e.attr.st_size = file_size;
    e.attr.st_ctime = ctime;
    e.attr.st_atime = ctime;
    e.attr.st_mtime = ctime;

    fuse_reply_entry (req, &e);
}

// FUSE lowlevel operation: lookup
// Valid replies: fuse_reply_entry() fuse_reply_err()
static void s3fuse_lookup (fuse_req_t req, fuse_ino_t parent, const char *name)
{
    S3Fuse *s3fuse = fuse_req_userdata (req);

    LOG_debug (FUSE_LOG, "lookup  name: %s parent inode: %d", name, parent);

    dir_tree_lookup (s3fuse->dir_tree, parent, name, s3fuse_lookup_cb, req);
}
/*}}}*/

/*{{{ open operation */

// FUSE lowlevel operation: open
// Valid replies: fuse_reply_open() fuse_reply_err()
static void s3fuse_open (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    S3Fuse *s3fuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, "open  inode: %d, flags: %d", ino, fi->flags);

    dir_tree_file_open (s3fuse->dir_tree, ino, fi);

    fuse_reply_open (req, fi);
}
/*}}}*/

/*{{{ create operation */
// create callback
void s3fuse_create_cb (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size, struct fuse_file_info *fi)
{
	struct fuse_entry_param e;

    LOG_debug (FUSE_LOG, "add_file_cb  success: %s", success?"YES":"NO");
    if (!success) {
		fuse_reply_err (req, ENOENT);
        return;
    }

    memset(&e, 0, sizeof(e));
    e.ino = ino;
    e.attr_timeout = 1.0;
    e.entry_timeout = 1.0;

    e.attr.st_ino = ino;
    e.attr.st_mode = mode;
	e.attr.st_nlink = 1;
	e.attr.st_size = file_size;

    fuse_reply_create (req, &e, fi);
}

// FUSE lowlevel operation: create
// Valid replies: fuse_reply_create() fuse_reply_err()
static void s3fuse_create (fuse_req_t req, fuse_ino_t parent_inode, const char *name, mode_t mode, struct fuse_file_info *fi)
{
    S3Fuse *s3fuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, "create  parent_inode: %d, name: %s, mode: %d ", parent_inode, name, mode);

    //dir_tree_add_file (s3fuse->dir_tree, parent_inode, name, mode, s3fuse_create_file_cb, req, fi);
    dir_tree_add_file (s3fuse->dir_tree, parent_inode, name, mode, NULL, req, fi);
}
/*}}}*/

/*{{{ release operation */

// FUSE lowlevel operation: release
// Valid replies: fuse_reply_err()
static void s3fuse_release (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    S3Fuse *s3fuse = fuse_req_userdata (req);

    LOG_debug (FUSE_LOG, "release  inode: %d, flags: %d", ino, fi->flags);

    dir_tree_file_release (s3fuse->dir_tree, ino, fi);
}
/*}}}*/

/*{{{ read operation */

// read callback
static void s3fuse_read_cb (fuse_req_t req, gboolean success, size_t max_size, off_t off, const char *buf, size_t buf_size)
{

    LOG_debug (FUSE_LOG, "read_cb  success: %s ", success?"YES":"NO");

    if (!success) {
		fuse_reply_err (req, ENOENT);
        return;
    }

	if (off < buf_size)
		fuse_reply_buf (req, buf + off, min (buf_size - off, max_size));
	else
	    fuse_reply_buf (req, NULL, 0);
}

// FUSE lowlevel operation: read
// Valid replies: fuse_reply_buf() fuse_reply_err()
static void s3fuse_read (fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi)
{
    S3Fuse *s3fuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, "read  inode: %"INO_FMT", size: %zd, off: %"OFF_FMT, ino, size, off);

    dir_tree_file_read (s3fuse->dir_tree, ino, size, off, s3fuse_read_cb, req, fi);
}
/*}}}*/

// write callback
static void s3fuse_write_cb (fuse_req_t req, gboolean success, size_t count)
{
    LOG_debug (FUSE_LOG, "write_cb  success: %s", success?"YES":"NO");

    if (!success) {
		fuse_reply_err (req, ENOENT);
        return;
    }
    
    fuse_reply_write (req, count);
}
// FUSE lowlevel operation: write
// Valid replies: fuse_reply_write() fuse_reply_err()
static void s3fuse_write (fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi)
{
    S3Fuse *s3fuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, "write  inode: %d, size: %zd, off: %ld ", ino, size, off);

    dir_tree_file_write (s3fuse->dir_tree, ino, buf, size, off, s3fuse_write_cb, req, fi);
}


