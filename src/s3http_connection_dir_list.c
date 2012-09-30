#include "include/s3http_connection.h"
#include "include/dir_tree.h"

typedef struct {
    Application *app;
    DirTree *dir_tree;
    S3HttpConnection *con;
    gchar *resource_path;
    gchar *dir_path;
    fuse_ino_t ino;
    gint max_keys;
    S3HttpConnection_directory_listing_callback directory_listing_callback;
    gpointer callback_data;
} DirListRequest;

#define CON_DIR_LOG "con_dir"

// parses S3 directory XML 
// reutrns TRUE if ok
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
    gchar *name = NULL;
    gchar *size;

    doc = xmlReadMemory (xml, xml_len, "", NULL, 0);
    if (doc == NULL)
        return FALSE;

    ctx = xmlXPathNewContext (doc);
    xmlXPathRegisterNs (ctx, (xmlChar *) "s3", (xmlChar *) "http://s3.amazonaws.com/doc/2006-03-01/");

    // files
    contents_xp = xmlXPathEvalExpression ((xmlChar *) "//s3:Contents", ctx);
    content_nodes = contents_xp->nodesetval;
    for(i = 0; i < content_nodes->nodeNr; i++) {
        char *bname;
        ctx->node = content_nodes->nodeTab[i];

        // object name
        key = xmlXPathEvalExpression ((xmlChar *) "s3:Key", ctx);
        key_nodes = key->nodesetval;
        name = (gchar *)xmlNodeListGetString (doc, key_nodes->nodeTab[0]->xmlChildrenNode, 1);
        
        key = xmlXPathEvalExpression ((xmlChar *) "s3:Size", ctx);
        key_nodes = key->nodesetval;
        size = (gchar *)xmlNodeListGetString (doc, key_nodes->nodeTab[0]->xmlChildrenNode, 1);
        
        if (!strcmp (name, dir_list->dir_path))
            continue;

        bname = strstr (name, dir_list->dir_path);
        bname = bname + strlen (dir_list->dir_path);

        bname = strstr (name, dir_list->dir_path);
        bname = bname + strlen (dir_list->dir_path);
        dir_tree_update_entry (dir_list->dir_tree, dir_list->dir_path, DET_file, dir_list->ino, bname, atoll (size));

        xmlXPathFreeObject(key);
    }
    xmlXPathFreeObject (contents_xp);

    // directories
    subdirs_xp = xmlXPathEvalExpression ((xmlChar *) "//s3:CommonPrefixes", ctx);
    subdir_nodes = subdirs_xp->nodesetval;
    for(i = 0; i < subdir_nodes->nodeNr; i++) {
        char *bname;

        ctx->node = subdir_nodes->nodeTab[i];

        // object name
        key = xmlXPathEvalExpression((xmlChar *) "s3:Prefix", ctx);
        key_nodes = key->nodesetval;
        name = (gchar *)xmlNodeListGetString (doc, key_nodes->nodeTab[0]->xmlChildrenNode, 1);
            

        bname = strstr (name, dir_list->dir_path);
        bname = bname + strlen (dir_list->dir_path);
    
        //XXX: remove trailing '/' characters
        if (bname[strlen (bname) - 1] == '/')
            bname[strlen (bname) - 1] = '\0';
        
        dir_tree_update_entry (dir_list->dir_tree, dir_list->dir_path, DET_dir, dir_list->ino, bname, 0);

        xmlXPathFreeObject(key);
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
    char *next_marker;

    doc = xmlReadMemory (xml, xml_len, "", NULL, 0);
    ctx = xmlXPathNewContext (doc);
    xmlXPathRegisterNs (ctx, (xmlChar *) "s3", (xmlChar *) "http://s3.amazonaws.com/doc/2006-03-01/");
    marker_xp = xmlXPathEvalExpression ((xmlChar *) "//s3:NextMarker", ctx);
    nodes = marker_xp->nodesetval;

    if (!nodes || nodes->nodeNr < 1) {
        next_marker = NULL;
    } else {
        next_marker = (char *) xmlNodeListGetString (doc, nodes->nodeTab[0]->xmlChildrenNode, 1);
    }

    xmlXPathFreeObject(marker_xp);
    xmlXPathFreeContext(ctx);
    xmlFreeDoc(doc);

    return next_marker;
}

// error, return error to fuse 
static void s3http_connection_on_directory_listing_error (void *ctx)
{
    DirListRequest *dir_req = (DirListRequest *) ctx;
    
    LOG_err (CON_DIR_LOG, "Failed to retrieve directory listing !");

    // we are done, stop updating
    dir_tree_stop_update (dir_req->dir_tree, dir_req->ino);
    
    if (dir_req->directory_listing_callback)
        dir_req->directory_listing_callback (dir_req->callback_data, FALSE);

    g_free (dir_req);
}

// Directory read callback function
static void s3http_connection_on_directory_listing_data (void *ctx, const gchar *buf, size_t buf_len)
{   
    DirListRequest *dir_req = (DirListRequest *) ctx;
    const gchar *next_marker;
    gchar *req_path;
    gboolean res;
   
    if (!buf_len) {
        LOG_err (CON_DIR_LOG, "Directory buffer is empty !");
        s3http_connection_on_directory_listing_error ((void *) dir_req);
        return;
    }
   
    parse_dir_xml (dir_req, buf, buf_len);
    
    // repeat starting from the mark
    next_marker = get_next_marker (buf, buf_len);

    // check if we need to get more data
    if (!strstr (buf, "<IsTruncated>true</IsTruncated>") && !next_marker) {
        LOG_debug (CON_DIR_LOG, "DONE !!");
        
        if (dir_req->directory_listing_callback)
            dir_req->directory_listing_callback (dir_req->callback_data, TRUE);
        
        // we are done, stop updating
        dir_tree_stop_update (dir_req->dir_tree, dir_req->ino);

        g_free (dir_req);
        return;
    }

    // execute HTTP request
    req_path = g_strdup_printf ("/?delimiter=/&prefix=%s&max-keys=%d&marker=%s", dir_req->dir_path, dir_req->max_keys, next_marker);
    
    res = s3http_connection_make_request (dir_req->con, 
        dir_req->resource_path, req_path, "GET", NULL,
        s3http_connection_on_directory_listing_data,
        s3http_connection_on_directory_listing_error, 
        dir_req
    );
    g_free (req_path);

    if (!res) {
        LOG_err (CON_DIR_LOG, "Failed to create HTTP request !");
        s3http_connection_on_directory_listing_error ((void *) dir_req);
        return;
    }
}

// create DirListRequest
gboolean s3http_connection_get_directory_listing (S3HttpConnection *con, const gchar *dir_path, fuse_ino_t ino,
    S3HttpConnection_directory_listing_callback directory_listing_callback, gpointer callback_data)
{
    DirListRequest *dir_req;
    gchar *req_path;
    gboolean res;

    LOG_debug (CON_DIR_LOG, "Getting directory listing for: %s", dir_path);

    dir_req = g_new0 (DirListRequest, 1);
    dir_req->con = con;
    dir_req->app = s3http_connection_get_app (con);
    dir_req->dir_tree = application_get_dir_tree (dir_req->app);
    dir_req->ino = ino;
    // XXX: settings
    dir_req->max_keys = 1000;
    dir_req->directory_listing_callback = directory_listing_callback;
    dir_req->callback_data = callback_data;
    
    // inform that we started to update the directory
    dir_tree_start_update (dir_req->dir_tree, dir_path);

    
    //XXX: fix dir_path
    if (!strcmp (dir_path, "/")) {
        dir_req->dir_path = g_strdup ("");
        dir_req->resource_path = g_strdup_printf ("/");
    } else {
        dir_req->dir_path = g_strdup_printf ("%s/", dir_path);
        dir_req->dir_path = dir_req->dir_path + 1;
        dir_req->resource_path = g_strdup_printf ("/");
    }
   
    req_path = g_strdup_printf ("/?delimiter=/&prefix=%s&max-keys=%d", dir_req->dir_path, dir_req->max_keys);

    res = s3http_connection_make_request (con, 
        dir_req->resource_path, req_path, "GET", NULL,
        s3http_connection_on_directory_listing_data,
        s3http_connection_on_directory_listing_error, 
        dir_req
    );
    
    g_free (req_path);

    if (!res) {
        LOG_err (CON_DIR_LOG, "Failed to create HTTP request !");
        s3http_connection_on_directory_listing_error ((void *) dir_req);
        return FALSE;
    }

    return TRUE;
}
