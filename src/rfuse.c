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
#include "rfuse.h"
#include "dir_tree.h"

// error codes: /usr/include/asm/errno.h /usr/include/asm-generic/errno-base.h

/*{{{ struct / defines */

struct _RFuse {
    Application *app;
    DirTree *dir_tree;
    gchar *mountpoint;
    
    // the session that we use to process the fuse stuff
    struct fuse_session *session;
    struct fuse_chan *chan;
    // the event that we use to receive requests
    struct event *ev;
    struct event *ev_timer;
#if FUSE_USE_VERSION >= 30
    struct fuse_buf fbuf;
#else
    // what our receive-message length is
    size_t recv_size;
    // the buffer that we use to receive events
    char *recv_buf;
#endif
    
    // statistics
    guint64 read_ops;
    guint64 write_ops;
    guint64 readdir_ops;
    guint64 lookup_ops;
};

#define FUSE_LOG "fuse"
/*}}}*/

/*{{{ func declarations */
static void rfuse_init (void *userdata, struct fuse_conn_info *conn);
static void rfuse_on_read (evutil_socket_t fd, short what, void *arg);
static void rfuse_readdir (fuse_req_t req, fuse_ino_t ino, 
    size_t size, off_t off, struct fuse_file_info *fi);
static void rfuse_lookup (fuse_req_t req, fuse_ino_t parent_ino, const char *name);
static void rfuse_getattr (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
static void rfuse_setattr (fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi);
static void rfuse_open (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
static void rfuse_release (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
static void rfuse_read (fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
static void rfuse_write (fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi);
static void rfuse_create (fuse_req_t req, fuse_ino_t parent_ino, const char *name, mode_t mode, struct fuse_file_info *fi);
static void rfuse_forget (fuse_req_t req, fuse_ino_t ino, unsigned long nlookup);
static void rfuse_unlink (fuse_req_t req, fuse_ino_t parent_ino, const char *name);
static void rfuse_mkdir (fuse_req_t req, fuse_ino_t parent_ino, const char *name, mode_t mode);
static void rfuse_rmdir (fuse_req_t req, fuse_ino_t parent_ino, const char *name);
//static void rfuse_on_timer (evutil_socket_t fd, short what, void *arg);
static void rfuse_rename (fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname);
static void rfuse_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size);
static void rfuse_listxattr (fuse_req_t req, fuse_ino_t ino, size_t size);

static struct fuse_lowlevel_ops rfuse_opers = {
    .init       = rfuse_init,
    .readdir    = rfuse_readdir,
    .lookup     = rfuse_lookup,
    .getattr    = rfuse_getattr,
    .setattr    = rfuse_setattr,
    .open       = rfuse_open,
    .release    = rfuse_release,
    .read       = rfuse_read,
    .write      = rfuse_write,
    .create     = rfuse_create,
    .forget     = rfuse_forget,
    .unlink     = rfuse_unlink,
    .mkdir      = rfuse_mkdir,
    .rmdir      = rfuse_rmdir,
    .rename     = rfuse_rename,
    .getxattr   = rfuse_getxattr,
    .listxattr  = rfuse_listxattr,
};
/*}}}*/

/*{{{ create / destroy */

// create RFuse object
// create fuse handle and add it to libevent polling
RFuse *rfuse_new (Application *app, const gchar *mountpoint, const gchar *fuse_opts)
{
    RFuse *rfuse;
    //struct timeval tv;
    struct fuse_args args = FUSE_ARGS_INIT (0, NULL);

    rfuse = g_new0 (RFuse, 1);
    rfuse->app = app;
    rfuse->dir_tree = application_get_dir_tree (app);
    rfuse->mountpoint = g_strdup (mountpoint);
    rfuse->read_ops = rfuse->write_ops = rfuse->readdir_ops = rfuse->lookup_ops = 0;

    if (fuse_opts) {
        if (fuse_opt_add_arg (&args, "riofs") == -1) {
            LOG_err (FUSE_LOG, "Failed to parse FUSE parameter !");
            return NULL;
        }

        if (fuse_opt_add_arg (&args, "-o") == -1) {
            LOG_err (FUSE_LOG, "Failed to parse FUSE parameter !");
            return NULL;
        }

        if (fuse_opt_add_arg (&args, fuse_opts) == -1) {
            LOG_err (FUSE_LOG, "Failed to parse FUSE parameter !");
            return NULL;
        }
    }

    if ((rfuse->chan = fuse_mount (rfuse->mountpoint, &args)) == NULL) {
        LOG_err (FUSE_LOG, "Failed to mount FUSE partition !");
        return NULL;
    }
    fuse_opt_free_args (&args);

#if FUSE_USE_VERSION >= 30
    rfuse->fbuf.mem = NULL;
#else
    // the receive buffer stuff
    rfuse->recv_size = fuse_chan_bufsize (rfuse->chan);

    // allocate the recv buffer
    if ((rfuse->recv_buf = g_malloc (rfuse->recv_size)) == NULL) {
        LOG_err (FUSE_LOG, "Failed to allocate memory !");
        return NULL;
    }
#endif
    
    // allocate a low-level session
    rfuse->session = fuse_lowlevel_new (NULL, &rfuse_opers, sizeof (rfuse_opers), rfuse);
    if (!rfuse->session) {
        LOG_err (FUSE_LOG, "Failed to init FUSE !");
        return NULL;
    }

    fuse_session_add_chan (rfuse->session, rfuse->chan);

    rfuse->ev = event_new (application_get_evbase (app), 
        fuse_chan_fd (rfuse->chan), EV_READ, &rfuse_on_read, 
        rfuse
    );
    if (!rfuse->ev) {
        LOG_err (FUSE_LOG, "event_new");
        return NULL;
    }

    if (event_add (rfuse->ev, NULL)) {
        LOG_err (FUSE_LOG, "event_add");
        return NULL;
    }
    /*
    rfuse->ev_timer = evtimer_new (application_get_evbase (app), 
        &rfuse_on_timer, 
        rfuse
    );
    
    tv.tv_sec = 10;
    tv.tv_usec = 0;
    LOG_err (FUSE_LOG, "event_add");
    if (event_add (rfuse->ev_timer, &tv)) {
        LOG_err (FUSE_LOG, "event_add");
        return NULL;
    }
    */

    return rfuse;
}

void rfuse_destroy (RFuse *rfuse)
{
    fuse_unmount (rfuse->mountpoint, rfuse->chan);
    g_free (rfuse->mountpoint);

#if FUSE_USE_VERSION >= 30
    free (rfuse->fbuf.mem);
#else
    g_free (rfuse->recv_buf);
#endif
    event_free (rfuse->ev);
    fuse_session_destroy (rfuse->session);
    g_free (rfuse);
}

/*
static void rfuse_on_timer (evutil_socket_t fd, short what, void *arg)
{
    struct timeval tv;
    RFuse *rfuse = (RFuse *)arg;

    LOG_debug (FUSE_LOG, ">>>>>>>> On timer !!! :%d", event_pending (rfuse->ev, EV_TIMEOUT|EV_READ|EV_WRITE|EV_SIGNAL, NULL));
    event_base_dump_events (application_get_evbase (rfuse->app), stdout);
    tv.tv_sec = 1;
    tv.tv_usec = 0;

    if (fuse_session_exited (rfuse->session)) {
        LOG_err (FUSE_LOG, "No FUSE session !");
        return;
    }
    
    if (event_add (rfuse->ev_timer, &tv)) {
        LOG_err (FUSE_LOG, "event_add");
        return NULL;
    }
}
*/

// turn ASYNC read off
static void rfuse_init (G_GNUC_UNUSED void *userdata, struct fuse_conn_info *conn)
{
    conn->async_read = 0;
}

// low level fuse reading operations
static void rfuse_on_read (G_GNUC_UNUSED evutil_socket_t fd, G_GNUC_UNUSED short what, void *arg)
{
    RFuse *rfuse = (RFuse *)arg;
    struct fuse_chan *ch = rfuse->chan;
    int res;

    if (!ch) {
        LOG_err (FUSE_LOG, "No FUSE channel !");
        return;
    }

    if (fuse_session_exited (rfuse->session)) {
        LOG_err (FUSE_LOG, "No FUSE session !");
        return;
    }
    
    // loop until we complete a recv
    do {
        // a new fuse_req is available
#if FUSE_USE_VERSION >= 30
        res = fuse_session_receive_buf (rfuse->session, &rfuse->fbuf, ch);
#else
        res = fuse_chan_recv (&ch, rfuse->recv_buf, rfuse->recv_size);
#endif
    } while (res == -EINTR);

    if (res == 0)
        LOG_err (FUSE_LOG, "fuse_chan_recv gave EOF");

    if (res < 0 && res != -EAGAIN)
        LOG_err (FUSE_LOG, "fuse_chan_recv failed: %s", strerror(-res));
    
    if (res > 0) {
     //   LOG_debug (FUSE_LOG, "got %d bytes from /dev/fuse", res);

#if FUSE_USE_VERSION >= 30
        fuse_session_process_buf (rfuse->session, &rfuse->fbuf, ch);
#else
        fuse_session_process (rfuse->session, rfuse->recv_buf, res, ch);
#endif
    }
    
    // reschedule
    if (event_add (rfuse->ev, NULL))
        LOG_err (FUSE_LOG, "event_add");

    // ok, wait for the next event
    return;
}
/*}}}*/

/*{{{ readdir operation */

#define min(x, y) ((x) < (y) ? (x) : (y))

// return newly allocated buffer which holds directory entry
void rfuse_add_dirbuf (fuse_req_t req, struct dirbuf *b, const char *name, fuse_ino_t ino, off_t file_size)
{
    struct stat stbuf;
    size_t oldsize = b->size;

    if (!req)
        return;
    
    LOG_debug (FUSE_LOG, INO_H"add_dirbuf, name: %s", INO_T (ino), name);

    // get required buff size
    b->size += fuse_add_direntry (req, NULL, 0, name, NULL, 0);

    // extend buffer
    b->p = (char *) g_realloc (b->p, b->size);
    memset (&stbuf, 0, sizeof (stbuf));
    stbuf.st_ino = ino;
    stbuf.st_size = file_size;
    // add entry
    fuse_add_direntry (req, b->p + oldsize, b->size - oldsize, name, &stbuf, b->size);
}

// readdir callback
// Valid replies: fuse_reply_buf() fuse_reply_err()
static void rfuse_readdir_cb (fuse_req_t req, gboolean success, size_t max_size, off_t off, 
    const char *buf, size_t buf_size, G_GNUC_UNUSED gpointer ctx)
{
    LOG_debug (FUSE_LOG, "readdir_cb  success: %s, buf_size: %zd, size: %zd, off: %"OFF_FMT, 
        success?"YES":"NO", buf_size, max_size, off);

    if (!success) {
        fuse_reply_err (req, ENOTDIR);
        return;
    }

    if (off < (off_t)buf_size)
        fuse_reply_buf (req, buf + off, min (buf_size - off, max_size));
    else
        fuse_reply_buf (req, NULL, 0);
}

// FUSE lowlevel operation: readdir
// Valid replies: fuse_reply_buf() fuse_reply_err()
static void rfuse_readdir (fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, G_GNUC_UNUSED struct fuse_file_info *fi)
{
    RFuse *rfuse = fuse_req_userdata (req);

    LOG_debug (FUSE_LOG, INO_H"readdir inode, size: %zd, off: %"OFF_FMT, INO_T (ino), size, off);
    
    rfuse->readdir_ops++;
    // fill directory buffer for "ino" directory
    dir_tree_fill_dir_buf (rfuse->dir_tree, ino, size, off, rfuse_readdir_cb, req, NULL);
}
/*}}}*/

/*{{{ getattr operation */

// getattr callback
static void rfuse_getattr_cb (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size, time_t ctime)
{
    struct stat stbuf;

    LOG_debug (FUSE_LOG, INO_H"getattr_cb, success: %s", INO_T (ino), success?"YES":"NO");
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
static void rfuse_getattr (fuse_req_t req, fuse_ino_t ino, G_GNUC_UNUSED struct fuse_file_info *fi)
{
    RFuse *rfuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, INO_H"getattr", INO_T (ino));

    dir_tree_getattr (rfuse->dir_tree, ino, rfuse_getattr_cb, req);
}
/*}}}*/

/*{{{ setattr operation */
// setattr callback
static void rfuse_setattr_cb (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size)
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
static void rfuse_setattr (fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi)
{
    RFuse *rfuse = fuse_req_userdata (req);

    dir_tree_setattr (rfuse->dir_tree, ino, attr, to_set, rfuse_setattr_cb, req, fi);
}
/*}}}*/

/*{{{ lookup operation*/

// lookup callback
static void rfuse_lookup_cb (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size, time_t ctime)
{
    struct fuse_entry_param e;

    LOG_debug (FUSE_LOG, INO_H"lookup_cb, file size: %"OFF_FMT" success: %s", INO_T (ino), file_size, success?"YES":"NO");
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
static void rfuse_lookup (fuse_req_t req, fuse_ino_t parent_ino, const char *name)
{
    RFuse *rfuse = fuse_req_userdata (req);

    LOG_debug (FUSE_LOG, "lookup  name: %s parent inode: %"INO_FMT, name, INO parent_ino);

    rfuse->lookup_ops++;

    dir_tree_lookup (rfuse->dir_tree, parent_ino, name, rfuse_lookup_cb, req);
}
/*}}}*/

/*{{{ open operation */

static void rfuse_open_cb (fuse_req_t req, gboolean success, struct fuse_file_info *fi)
{
    if (success)
        fuse_reply_open (req, fi);
    else
        fuse_reply_err (req, ENOENT);
}

// FUSE lowlevel operation: open
// Valid replies: fuse_reply_open() fuse_reply_err()
static void rfuse_open (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    RFuse *rfuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, INO_FI_H"open inode, flags: %d", INO_T (ino), fi, fi->flags);

    dir_tree_file_open (rfuse->dir_tree, ino, fi, rfuse_open_cb, req);
}
/*}}}*/

/*{{{ create operation */
// create callback
void rfuse_create_cb (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size, struct fuse_file_info *fi)
{
    struct fuse_entry_param e;

    LOG_debug (FUSE_LOG, INO_FI_H"add_file_cb  success: %s", INO_T (ino), fi, success?"YES":"NO");
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
static void rfuse_create (fuse_req_t req, fuse_ino_t parent_ino, const char *name, mode_t mode, struct fuse_file_info *fi)
{
    RFuse *rfuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, "create  parent inode: %"INO_FMT", name: %s, mode: %d ", INO parent_ino, name, mode);

    dir_tree_file_create (rfuse->dir_tree, parent_ino, name, mode, rfuse_create_cb, req, fi);
}
/*}}}*/

/*{{{ release operation */

// FUSE lowlevel operation: release
// Valid replies: fuse_reply_err()
static void rfuse_release (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    RFuse *rfuse = fuse_req_userdata (req);

    LOG_debug (FUSE_LOG, INO_FI_H"release  inode, flags: %d", INO_T (ino), fi, fi->flags);

    dir_tree_file_release (rfuse->dir_tree, ino, fi);

    fuse_reply_err (req, 0);
}
/*}}}*/

/*{{{ read operation */

// read callback
static void rfuse_read_cb (fuse_req_t req, gboolean success, const char *buf, size_t buf_size)
{

    LOG_debug (FUSE_LOG, "[req: %p] <<<<< read_cb  success: %s IN buf: %zu", req, success?"YES":"NO", buf_size);

    if (!success) {
        fuse_reply_err (req, ENOENT);
        return;
    }

    fuse_reply_buf (req, buf, buf_size);
}

// FUSE lowlevel operation: read
// Valid replies: fuse_reply_buf() fuse_reply_err()
static void rfuse_read (fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi)
{
    RFuse *rfuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, INO_FI_H">>>> read  inode, size: %zd, off: %"OFF_FMT, INO_T (ino), fi, size, off);

    rfuse->read_ops++;
    dir_tree_file_read (rfuse->dir_tree, ino, size, off, rfuse_read_cb, req, fi);
}
/*}}}*/

/*{{{ write operation */
// write callback
static void rfuse_write_cb (fuse_req_t req, gboolean success, size_t count)
{
    LOG_debug (FUSE_LOG, "[req: %p] write_cb  success: %s", req, success?"YES":"NO");

    if (!success) {
        fuse_reply_err (req, ENOENT);
        return;
    }
    
    fuse_reply_write (req, count);
}
// FUSE lowlevel operation: write
// Valid replies: fuse_reply_write() fuse_reply_err()
static void rfuse_write (fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi)
{
    RFuse *rfuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, INO_FI_H"write inode, size: %zd, off: %"OFF_FMT, INO_T (ino), fi, size, off);

    rfuse->write_ops++;
    dir_tree_file_write (rfuse->dir_tree, ino, buf, size, off, rfuse_write_cb, req, fi);
}
/*}}}*/

/*{{{ forget operation*/

// forget callback
static void rfuse_forget_cb (fuse_req_t req, gboolean success)
{
    if (success)
        fuse_reply_none (req);
    else
        fuse_reply_none (req);
}

// Forget about an inode
// Valid replies: fuse_reply_none
// XXX: it removes files and directories
static void rfuse_forget (fuse_req_t req, fuse_ino_t ino, unsigned long nlookup)
{
    RFuse *rfuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, INO_H"forget nlookup: %lu", INO_T (ino), nlookup);
    
    if (nlookup != 0) {
        LOG_debug (FUSE_LOG, "Ignoring forget with nlookup > 0");
        fuse_reply_none (req);
    } else
        dir_tree_file_remove (rfuse->dir_tree, ino, rfuse_forget_cb, req);
}
/*}}}*/

/*{{{ unlink operation*/

static void rfuse_unlink_cb (fuse_req_t req, gboolean success)
{
    LOG_debug (FUSE_LOG, "[%p] success: %s", req, success ? "TRUE" : "FALSE");

    if (success)
        fuse_reply_err (req, 0);
    else
        fuse_reply_err (req, ENOENT);
}

// Remove a file
// Valid replies: fuse_reply_err
// XXX: not used, see rfuse_forget
static void rfuse_unlink (fuse_req_t req, fuse_ino_t parent, const char *name)
{
    RFuse *rfuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, "[%p] unlink  parent_ino: %"INO_FMT", name: %s", req, INO parent, name);

    dir_tree_file_unlink (rfuse->dir_tree, parent, name, rfuse_unlink_cb, req);
}
/*}}}*/

/*{{{ mkdir operator */

// mkdir callback
static void rfuse_mkdir_cb (fuse_req_t req, gboolean success, fuse_ino_t ino, int mode, off_t file_size, time_t ctime)
{
    struct fuse_entry_param e;

    LOG_debug (FUSE_LOG, "mkdir_cb  success: %s, ino: %"INO_FMT, success?"YES":"NO", INO ino);
    if (!success) {
        fuse_reply_err (req, ENOENT);
        return;
    }

    memset(&e, 0, sizeof(e));
    e.ino = ino;
    e.attr_timeout = 1.0;
    e.entry_timeout = 1.0;
    //e.attr.st_mode = S_IFDIR | 0755;
    e.attr.st_mode = mode;
    e.attr.st_nlink = 1;
    e.attr.st_ctime = ctime;
    e.attr.st_atime = ctime;
    e.attr.st_mtime = ctime;
    
    e.attr.st_ino = ino;
    e.attr.st_size = file_size;
    
    fuse_reply_entry (req, &e);
}

// Create a directory
// Valid replies: fuse_reply_entry fuse_reply_err
static void rfuse_mkdir (fuse_req_t req, fuse_ino_t parent_ino, const char *name, mode_t mode)
{
    RFuse *rfuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, "mkdir  parent_ino: %"INO_FMT", name: %s, mode: %d", INO parent_ino, name, mode);

    dir_tree_dir_create (rfuse->dir_tree, parent_ino, name, mode, rfuse_mkdir_cb, req);
}
/*}}}*/

/*{{{ rmdir operator */

// Remove a directory
// Valid replies: fuse_reply_err
// XXX: not used, see rfuse_forget
static void rfuse_rmdir (fuse_req_t req, fuse_ino_t parent_ino, const char *name)
{
    RFuse *rfuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, "[%p] rmdir  parent_ino: %"INO_FMT", name: %s", rfuse, INO parent_ino, name);

    fuse_reply_err (req, 0);
}
/*}}}*/

/*{{{ rename operator */

static void rfuse_rename_cb (fuse_req_t req, gboolean success)
{
    LOG_debug (FUSE_LOG, "rename_cb  success: %s", success?"YES":"NO");

    if (!success) {
        fuse_reply_err (req, EPERM);
        return;
    }
    
    fuse_reply_err (req, 0);
}

// Rename file or directory
// Valid replies: fuse_reply_err
static void rfuse_rename (fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname)
{
    RFuse *rfuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, "rename  parent_ino: %"INO_FMT", name: %s new_parent_in: %"INO_FMT", newname: %s", 
        INO parent, name, INO newparent, newname);
   
    dir_tree_rename (rfuse->dir_tree, parent, name, newparent, newname, rfuse_rename_cb, req);
}
/*}}}*/

/*{{{ listxattr operator*/
static void rfuse_listxattr (fuse_req_t req, fuse_ino_t ino, size_t size)
{
    RFuse *rfuse = fuse_req_userdata (req);
    // XXX: move to DirTree
    gchar attr_list[] = "user.version\0user.etag\0user.content_type\0";
    
    LOG_debug (FUSE_LOG, INO_H"listxattr, size: %zu", INO_T (ino), size);

    (void) rfuse;
    if (size == 0) {
        fuse_reply_xattr (req, sizeof (attr_list));
    } else {
        fuse_reply_buf (req, attr_list, sizeof (attr_list));
    }
}
/*}}}*/

/*{{{ getxattr operator */

static void rfuse_getxattr_cb (fuse_req_t req, gboolean success, fuse_ino_t ino, const gchar *str, size_t size)
{
    LOG_debug (FUSE_LOG, INO_H"getattr_cb  success: %s  str: %s", INO_T (ino), success?"YES":"NO", str);
    
    if (!success || !str) {
        fuse_reply_err (req, ENOTSUP);
        return;
    }

    if (size == 0) {
        fuse_reply_xattr (req, strlen (str));
    } else {
        fuse_reply_buf (req, str, strlen (str));
    }
}

static void rfuse_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size)
{
    RFuse *rfuse = fuse_req_userdata (req);
    
    LOG_debug (FUSE_LOG, "getxattr  for: %"INO_FMT" attr name: %s size: %zu", INO ino, name, size);

    dir_tree_getxattr (rfuse->dir_tree, ino, name, size, rfuse_getxattr_cb, req);
}
/*}}}*/

/*{{{ get_stats */
void rfuse_get_stats (RFuse *rfuse, guint64 *read_ops, guint64 *write_ops, guint64 *readdir_ops, guint64 *lookup_ops)
{
    if (!rfuse)
        return;

    *read_ops = rfuse->read_ops;
    *write_ops = rfuse->write_ops;
    *readdir_ops = rfuse->readdir_ops;
    *lookup_ops = rfuse->lookup_ops;
}
/*}}}*/
