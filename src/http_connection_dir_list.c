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
#include "http_connection.h"
#include "dir_tree.h"

typedef struct {
    Application *app;
    DirTree *dir_tree;
    HttpConnection *con;
    gchar *dir_path;
    fuse_ino_t ino;
    HttpConnection_directory_listing_callback directory_listing_callback;
    gpointer callback_data;
    guint max_keys;
} DirListRequest;

#define CON_DIR_LOG "con_dir"

// parses  directory XML 
// returns TRUE if ok
static gboolean parse_dir_xml (DirListRequest *dir_list, const char *xml, size_t xml_len)
{
    xmlDocPtr doc;
    xmlXPathContextPtr ctx;
    xmlXPathObjectPtr contents_xp;
    xmlNodeSetPtr content_nodes;
    xmlXPathObjectPtr subdirs_xp;
    xmlNodeSetPtr subdir_nodes;
    int i;
    xmlXPathObjectPtr key;
    xmlNodeSetPtr key_nodes;

// checks value for NULL, continue if it fails
#define XML_VAR_CHK(v) \
    do { \
        if ((v) == NULL) { \
            LOG_err (CON_DIR_LOG, "S3 returned incorrect XML !"); \
            continue; \
        } \
    } while (0)
// checks value for NULL, continue if it fails, free key to avoid memleaks
#define XML_VAR_CHK_KEY(v) \
    do { \
        if ((v) == NULL) { \
            LOG_err (CON_DIR_LOG, "S3 returned incorrect XML !"); \
            xmlXPathFreeObject (key); \
            continue; \
        } \
    } while (0)

    doc = xmlReadMemory (xml, xml_len, "", NULL, 0);
    if (doc == NULL)
        return FALSE;

    ctx = xmlXPathNewContext (doc);
    if (!ctx) {
        LOG_err (CON_DIR_LOG, "S3 returned incorrect XML !");
        xmlFreeDoc (doc);
        return FALSE;
    }
    xmlXPathRegisterNs (ctx, (xmlChar *) "s3", (xmlChar *) "http://s3.amazonaws.com/doc/2006-03-01/");

    // files
    contents_xp = xmlXPathEvalExpression ((xmlChar *) "//s3:Contents", ctx);
    if (!contents_xp) {
        LOG_err (CON_DIR_LOG, "S3 returned incorrect XML !");
        xmlXPathFreeContext (ctx);
        xmlFreeDoc (doc);
        return FALSE;
    }

    content_nodes = contents_xp->nodesetval;
    if (!content_nodes) {
        LOG_err (CON_DIR_LOG, "S3 returned incorrect XML !");
        xmlXPathFreeObject (contents_xp);
        xmlXPathFreeContext (ctx);
        xmlFreeDoc (doc);
        return FALSE;
    }

    for(i = 0; i < content_nodes->nodeNr; i++) {
        gchar *bname = NULL;
        gint64 size = 0;
        time_t last_modified = time (NULL);
        gchar *name = NULL;
        gchar *s_size = NULL;
        gchar *s_last_modified = NULL;

        ctx->node = content_nodes->nodeTab[i];

        // object name
        key = xmlXPathEvalExpression ((xmlChar *) "s3:Key", ctx);
        XML_VAR_CHK (key);
        key_nodes = key->nodesetval;
        XML_VAR_CHK_KEY (key_nodes);
        name = (gchar *)xmlNodeListGetString (doc, key_nodes->nodeTab[0]->xmlChildrenNode, 1);
        xmlXPathFreeObject (key);
        XML_VAR_CHK (name);
        
        key = xmlXPathEvalExpression ((xmlChar *) "s3:Size", ctx);
        XML_VAR_CHK (key);
        key_nodes = key->nodesetval;
        XML_VAR_CHK_KEY (key_nodes);
        s_size = (gchar *)xmlNodeListGetString (doc, key_nodes->nodeTab[0]->xmlChildrenNode, 1);
        if (s_size) {
            size = strtoll (s_size, NULL, 10);
            xmlFree (s_size);
        }
        xmlXPathFreeObject (key);

        key = xmlXPathEvalExpression ((xmlChar *) "s3:LastModified", ctx);
        XML_VAR_CHK (key);
        key_nodes = key->nodesetval;
        XML_VAR_CHK_KEY (key_nodes);
        s_last_modified = (gchar *)xmlNodeListGetString (doc, key_nodes->nodeTab[0]->xmlChildrenNode, 1);
        if (s_last_modified) {
            struct tm tmp = {0};
            // 2013-04-11T15:16
            if (strptime (s_last_modified, "%Y-%m-%dT%H:%M:%S", &tmp))
                last_modified = mktime (&tmp);
            xmlFree (s_last_modified);
        }
        xmlXPathFreeObject (key);
        
        // 
        if (!strncmp (name, dir_list->dir_path, strlen (name))) {
            xmlFree (name);
            continue;
        }

        bname = strstr (name, dir_list->dir_path);
        bname = bname + strlen (dir_list->dir_path);

        if (strlen (bname) == 1 && bname[0] == '/')  {
            LOG_debug (CON_DIR_LOG, "Wrong file name !");
            xmlFree (name);
            continue;
        }

        // make sure size is set correctly
        if (size < 0) {
            LOG_err (CON_DIR_LOG, "S3 returned incorrect file size for %s", bname);
            size = 0;
        }

        dir_tree_update_entry (dir_list->dir_tree, dir_list->dir_path, DET_file, dir_list->ino, 
            bname, size, last_modified);
        
        xmlFree (name);
    }

    xmlXPathFreeObject (contents_xp);

    // directories
    subdirs_xp = xmlXPathEvalExpression ((xmlChar *) "//s3:CommonPrefixes", ctx);
    if (!subdirs_xp) {
        xmlXPathFreeContext (ctx);
        xmlFreeDoc (doc);
        return TRUE;
    }

    subdir_nodes = subdirs_xp->nodesetval;
    if (!subdir_nodes) {
        xmlXPathFreeObject (subdirs_xp);
        xmlXPathFreeContext (ctx);
        xmlFreeDoc (doc);
        return TRUE;
    }

    for(i = 0; i < subdir_nodes->nodeNr; i++) {
        gchar *bname = NULL;
        gchar *name = NULL;
        time_t last_modified;

        ctx->node = subdir_nodes->nodeTab[i];

        // object name
        key = xmlXPathEvalExpression((xmlChar *) "s3:Prefix", ctx);
        XML_VAR_CHK (key);
        key_nodes = key->nodesetval;
        XML_VAR_CHK_KEY (key_nodes);
        name = (gchar *)xmlNodeListGetString (doc, key_nodes->nodeTab[0]->xmlChildrenNode, 1);
        xmlXPathFreeObject (key);
        XML_VAR_CHK (name);

        bname = strstr (name, dir_list->dir_path);
        bname = bname + strlen (dir_list->dir_path);
    
        //XXX: remove trailing '/' characters
        if (strlen (bname) > 1 && bname[strlen (bname) - 1] == '/') {
            bname[strlen (bname) - 1] = '\0';
        // XXX: 
        } else if (strlen (bname) == 1 && bname[0] == '/')  {
            LOG_debug (CON_DIR_LOG, "Wrong directory name !");
            xmlFree (name);
            continue;
        }
        
        // XXX: save / restore directory mtime
        last_modified = time (NULL);

        dir_tree_update_entry (dir_list->dir_tree, dir_list->dir_path, DET_dir, dir_list->ino, bname, 0, last_modified);

        xmlFree (name);
    }

    xmlXPathFreeObject (subdirs_xp);

    xmlXPathFreeContext (ctx);
    xmlFreeDoc (doc);

    return TRUE;
}

static const char *get_next_marker(const char *xml, size_t xml_len) {
    xmlDocPtr doc;
    xmlXPathContextPtr ctx;
    xmlXPathObjectPtr marker_xp;
    xmlNodeSetPtr nodes;
    char *next_marker = NULL;

    doc = xmlReadMemory (xml, xml_len, "", NULL, 0);
    if (!doc) {
        LOG_err (CON_DIR_LOG, "S3 returned incorrect XML !");
        return NULL;
    }

    ctx = xmlXPathNewContext (doc);
    if (!ctx) {
        xmlFreeDoc (doc);
        LOG_err (CON_DIR_LOG, "S3 returned incorrect XML !");
        return NULL;
    }

    xmlXPathRegisterNs (ctx, (xmlChar *) "s3", (xmlChar *) "http://s3.amazonaws.com/doc/2006-03-01/");
    marker_xp = xmlXPathEvalExpression ((xmlChar *) "//s3:NextMarker", ctx);
    if (!marker_xp) {
        LOG_err (CON_DIR_LOG, "S3 returned incorrect XML !");
        next_marker = NULL;
    } else {
        nodes = marker_xp->nodesetval;

        if (!nodes || nodes->nodeNr < 1) {
            next_marker = NULL;
        } else {
            next_marker = (char *) xmlNodeListGetString (doc, nodes->nodeTab[0]->xmlChildrenNode, 1);
        }

        xmlXPathFreeObject (marker_xp);
    }
    xmlXPathFreeContext (ctx);
    xmlFreeDoc (doc);

    return next_marker;
}


// free DirListRequest, release HTTPConnection, call callback function
static void directory_listing_done (HttpConnection *con, DirListRequest *dir_req, gboolean success)
{
    if (dir_req->directory_listing_callback)
        dir_req->directory_listing_callback (dir_req->callback_data, success);
        
    // we are done, stop updating
    dir_tree_stop_update (dir_req->dir_tree, dir_req->ino);
        
    // release HTTP client
    if (con)
        http_connection_release (con);

    g_free (dir_req->dir_path);
    g_free (dir_req);

}

// Directory read callback function
static void http_connection_on_directory_listing_data (HttpConnection *con, void *ctx, gboolean success,
        const gchar *buf, size_t buf_len, G_GNUC_UNUSED struct evkeyvalq *headers)
{   
    DirListRequest *dir_req = (DirListRequest *) ctx;
    const gchar *next_marker = NULL;
    gchar *req_path;
    gboolean res;
   
    if (!buf_len || !buf) {
        LOG_err (CON_DIR_LOG, INO_CON_H"Directory buffer is empty !", INO_T (dir_req->ino), con);
        directory_listing_done (con, dir_req, FALSE);
        return;
    }

    if (!success) {
        LOG_err (CON_DIR_LOG, INO_CON_H"Error getting directory list !", INO_T (dir_req->ino), con);
        directory_listing_done (con, dir_req, FALSE);
        return;
    }
   
    if (!parse_dir_xml (dir_req, buf, buf_len)) {
        LOG_err (CON_DIR_LOG, INO_CON_H"Error parsing directory XML !", INO_T (dir_req->ino), con);
        directory_listing_done (con, dir_req, FALSE);
        return;
    }
    
    // repeat starting from the mark
    next_marker = get_next_marker (buf, buf_len);

    // check if we need to get more data
    if (!g_strstr_len (buf, buf_len, "<IsTruncated>true</IsTruncated>") && !next_marker) {
        LOG_debug (CON_DIR_LOG, INO_CON_H"Directory listing done !", INO_T (dir_req->ino), con);
        directory_listing_done (con, dir_req, TRUE);
        return;
    }

    // execute HTTP request
    req_path = g_strdup_printf ("/?delimiter=/&prefix=%s&max-keys=%u&marker=%s", dir_req->dir_path, dir_req->max_keys, next_marker);
    
    xmlFree ((void *) next_marker);

    res = http_connection_make_request (dir_req->con, 
        req_path, "GET",
        NULL, TRUE, 0,
        http_connection_on_directory_listing_data,
        dir_req
    );
    g_free (req_path);

    if (!res) {
        LOG_err (CON_DIR_LOG, INO_CON_H"Failed to create HTTP request !", INO_T (dir_req->ino), con);
        directory_listing_done (con, dir_req, FALSE);
        return;
    }
}

// create DirListRequest
void http_connection_get_directory_listing (HttpConnection *con, const gchar *dir_path, fuse_ino_t ino,
    HttpConnection_directory_listing_callback directory_listing_callback, gpointer callback_data)
{
    DirListRequest *dir_req;
    gchar *req_path;
    gboolean res;

    LOG_debug (CON_DIR_LOG, INO_CON_H"Getting directory listing for: >>%s<<", INO_T (con), con, dir_path);

    dir_req = g_new0 (DirListRequest, 1);
    dir_req->con = con;
    dir_req->app = http_connection_get_app (con);
    dir_req->dir_tree = application_get_dir_tree (dir_req->app);
    dir_req->ino = ino;
    dir_req->max_keys = conf_get_uint (application_get_conf (con->app), "s3.keys_per_request");
    dir_req->directory_listing_callback = directory_listing_callback;
    dir_req->callback_data = callback_data;

    // acquire HTTP client
    http_connection_acquire (con);
    
    //XXX: fix dir_path
    if (!strlen (dir_path)) {
        dir_req->dir_path = g_strdup ("");
    } else {
        dir_req->dir_path = g_strdup_printf ("%s/", dir_path);
    }

    req_path = g_strdup_printf ("/?delimiter=/&max-keys=%u&prefix=%s", dir_req->max_keys, dir_req->dir_path);

    res = http_connection_make_request (con, 
        req_path, "GET",
        NULL, TRUE, 0,
        http_connection_on_directory_listing_data,
        dir_req
    );
    
    g_free (req_path);

    if (!res) {
        LOG_err (CON_DIR_LOG, INO_CON_H"Failed to create HTTP request !", INO_T (dir_req->ino), con);
        directory_listing_done (con, dir_req, FALSE);
        return;
    }

    return;
}
