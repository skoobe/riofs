/*
 * Copyright (C) 2012-2014 Paul Ionkin <paul.ionkin@gmail.com>
 * Copyright (C) 2012-2014 Skoobe GmbH. All rights reserved.
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
#include "utils.h"

gchar *get_random_string (size_t len, gboolean readable)
{
    gchar *out;

    out = g_malloc (len + 1);

    if (readable) {
        gchar readable_chars[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        size_t i;

        for (i = 0; i < len; i++)
            out[i] = readable_chars[rand() % strlen (readable_chars)];
    } else {
        if (!RAND_bytes ((unsigned char *)out, len))
            return out;
    }

    *(out + len) = '\0';

    return out;
}

gboolean get_md5_sum (const gchar *buf, size_t len, gchar **md5str, gchar **md5b)
{
    unsigned char digest[16];
    size_t i;
    gchar *out;

    if (!md5b && !md5str)
        return TRUE;

    MD5 ((const unsigned char *)buf, len, digest);


    if (md5b)
        *md5b = get_base64 ((const gchar *)digest, 16);
    if (md5str) {
        out = g_malloc (33);
        for (i = 0; i < 16; ++i)
            sprintf(&out[i*2], "%02x", (unsigned int)digest[i]);
        *md5str = out;
    }
    return TRUE;
}

gchar *get_base64 (const gchar *buf, size_t len)
{
    int ret;
    gchar *res;
    BIO *bmem, *b64;
    BUF_MEM *bptr;

    b64 = BIO_new (BIO_f_base64 ());
    bmem = BIO_new (BIO_s_mem ());
    b64 = BIO_push (b64, bmem);
    BIO_write (b64, buf, len);
    ret = BIO_flush (b64);
    if (ret != 1) {
        BIO_free_all (b64);
        return NULL;
    }
    BIO_get_mem_ptr (b64, &bptr);
    res = g_malloc (bptr->length);
    memcpy (res, bptr->data, bptr->length);
    res[bptr->length - 1] = '\0';
    BIO_free_all (b64);

    return res;
}

gboolean uri_is_https (const struct evhttp_uri *uri)
{
    const char *scheme;

    if (!uri)
        return FALSE;

    scheme = evhttp_uri_get_scheme (uri);
    if (!scheme)
        return FALSE;

    if (!strncasecmp ("https", scheme, 5))
        return TRUE;

    return FALSE;
}

gint uri_get_port (const struct evhttp_uri *uri)
{
    gint port;

    port = evhttp_uri_get_port (uri);

    // if no port is specified, libevent returns -1
    if (port == -1) {
        if (uri_is_https (uri))
            port = 443;
        else
            port = 80;
    }

    return port;
}

const gchar *http_find_header (const struct evkeyvalq *headers, const gchar *key)
{
    if (!headers || !key)
        return NULL;

    return evhttp_find_header (headers, key);
}

static int on_unlink_cb (const char *fpath, G_GNUC_UNUSED const struct stat *sb,
    G_GNUC_UNUSED int typeflag, G_GNUC_UNUSED struct FTW *ftwbuf)
{
    int rv = remove (fpath);

    if (rv)
        perror (fpath);

    return rv;
}

// remove directory tree
int utils_del_tree (const gchar *path, int depth)
{
    return nftw (path, on_unlink_cb, depth, FTW_DEPTH | FTW_PHYS);
}

guint64 timeval_diff (struct timeval *starttime, struct timeval *finishtime)
{
    guint64 msec = 0;

    // special case, when finishtime is not set
    if (!finishtime->tv_sec && !finishtime->tv_usec)
        return 0;

    if (finishtime->tv_sec > starttime->tv_sec) {
        msec = (guint64)((finishtime->tv_sec - starttime->tv_sec) * 1000);
        msec += (guint64)((finishtime->tv_usec - starttime->tv_usec) / 1000);
    } else if (finishtime->tv_usec > starttime->tv_usec) {
        msec = (guint64)((finishtime->tv_usec - starttime->tv_usec) / 1000);
    }

    return msec;
}

// removes leading and trailing double quotes from str
gchar *str_remove_quotes (gchar *str)
{
    gchar *start;
    size_t len;

    for (start = str; *start && *start == 0x22; start++);
    g_memmove (str, start, strlen (start) + 1);

    len = strlen (str);
    while (len--) {
        if (str[len] == 0x22)
            str[len] = '\0';
        else
            break;
    }

    return str;
}


// url encode utilities from wget sources

enum {
    /* rfc1738 reserved chars + "$" and ",".  */
    urlchr_reserved = 1,

    /* rfc1738 unsafe chars, plus non-printables.  */
    urlchr_unsafe   = 2
};

#define urlchr_test(c, mask) (urlchr_table[(unsigned char)(c)] & (mask))
#define URL_RESERVED_CHAR(c) urlchr_test(c, urlchr_reserved)
#define URL_UNSAFE_CHAR(c) urlchr_test(c, urlchr_unsafe)

/* Shorthands for the table: */
#define R  urlchr_reserved
#define U  urlchr_unsafe
#define RU R|U
#define XNUM_TO_DIGIT(x) ("0123456789ABCDEF"[x] + 0)
#define XNUM_TO_digit(x) ("0123456789abcdef"[x] + 0)

static const unsigned char urlchr_table[256] =
{
    U,  U,  U,  U,   U,  U,  U,  U,   /* NUL SOH STX ETX  EOT ENQ ACK BEL */
    U,  U,  U,  U,   U,  U,  U,  U,   /* BS  HT  LF  VT   FF  CR  SO  SI  */
    U,  U,  U,  U,   U,  U,  U,  U,   /* DLE DC1 DC2 DC3  DC4 NAK SYN ETB */
    U,  U,  U,  U,   U,  U,  U,  U,   /* CAN EM  SUB ESC  FS  GS  RS  US  */
    U,  0,  U, RU,   R,  U,  R,  0,   /* SP  !   "   #    $   %   &   '   */
    0,  0,  0, RU,   R,  0,  0,  R,   /* (   )   *   +    ,   -   .   /   */
    0,  0,  0,  0,   0,  0,  0,  0,   /* 0   1   2   3    4   5   6   7   */
    0,  0, RU,  R,   U,  R,  U,  R,   /* 8   9   :   ;    <   =   >   ?   */
    RU,  0,  0,  0,   0,  0,  0,  0,   /* @   A   B   C    D   E   F   G   */
    0,  0,  0,  0,   0,  0,  0,  0,   /* H   I   J   K    L   M   N   O   */
    0,  0,  0,  0,   0,  0,  0,  0,   /* P   Q   R   S    T   U   V   W   */
    0,  0,  0, RU,   U, RU,  U,  0,   /* X   Y   Z   [    \   ]   ^   _   */
    U,  0,  0,  0,   0,  0,  0,  0,   /* `   a   b   c    d   e   f   g   */
    0,  0,  0,  0,   0,  0,  0,  0,   /* h   i   j   k    l   m   n   o   */
    0,  0,  0,  0,   0,  0,  0,  0,   /* p   q   r   s    t   u   v   w   */
    0,  0,  0,  U,   U,  U,  0,  U,   /* x   y   z   {    |   }   ~   DEL */

    U, U, U, U,  U, U, U, U,  U, U, U, U,  U, U, U, U,
    U, U, U, U,  U, U, U, U,  U, U, U, U,  U, U, U, U,
    U, U, U, U,  U, U, U, U,  U, U, U, U,  U, U, U, U,
    U, U, U, U,  U, U, U, U,  U, U, U, U,  U, U, U, U,

    U, U, U, U,  U, U, U, U,  U, U, U, U,  U, U, U, U,
    U, U, U, U,  U, U, U, U,  U, U, U, U,  U, U, U, U,
    U, U, U, U,  U, U, U, U,  U, U, U, U,  U, U, U, U,
    U, U, U, U,  U, U, U, U,  U, U, U, U,  U, U, U, U,
};
#undef R
#undef U
#undef RU

/* The core of url_escape_* functions.  Escapes the characters that
   match the provided mask in urlchr_table.
*/
static char *url_escape_1 (const char *s, unsigned char mask)
{
    const char *p1;
    char *p2, *newstr;
    int newlen;
    int addition = 0;

    for (p1 = s; *p1; p1++)
        if (urlchr_test (*p1, mask))
            addition += 2;

    if (!addition)
        return g_strdup (s);

    newlen = (p1 - s) + addition;
    newstr = g_malloc0 (newlen + 1);

    p1 = s;
    p2 = newstr;
    while (*p1) {
        if (urlchr_test (*p1, mask)) {
            unsigned char c = *p1++;
            *p2++ = '%';
            *p2++ = XNUM_TO_DIGIT (c >> 4);
            *p2++ = XNUM_TO_DIGIT (c & 0xf);
        } else
            *p2++ = *p1++;
    }
    *p2 = '\0';

    return newstr;
}

/* URL-escape the unsafe characters (see urlchr_table) in a given
   string, returning a freshly allocated string.  */
char *url_escape (const char *s)
{
    return url_escape_1 (s, urlchr_unsafe);
}

// copy-paste from glib sources
void _queue_free_full (GQueue *queue, GDestroyNotify  free_func)
{
  g_queue_foreach (queue, (GFunc) free_func, NULL);
  g_queue_free (queue);
}
