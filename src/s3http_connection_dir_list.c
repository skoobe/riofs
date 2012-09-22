#include "include/s3http_connection.h"
#include "include/dir_tree.h"

typedef struct {
    Application *app;
    DirTree *dir_tree;
    S3HTTPConnection *con;
    gchar *resource_path;
    gchar *dir_path;
} DirListRequest;

// parses S3 directory XML 
// reutrns TRUE if ok
static gchar* parse_dir_xml (DirListRequest *dir_list, const char *xml, size_t xml_len)
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
    xmlXPathRegisterNs (ctx, (xmlChar *) "s3",
        (xmlChar *) "http://s3.amazonaws.com/doc/2006-03-01/");

    // files
    contents_xp = xmlXPathEvalExpression ((xmlChar *) "//s3:Contents", ctx);
    content_nodes = contents_xp->nodesetval;
    for(i = 0; i < content_nodes->nodeNr; i++) {
        ctx->node = content_nodes->nodeTab[i];

        // object name
        key = xmlXPathEvalExpression((xmlChar *) "s3:Key", ctx);
        key_nodes = key->nodesetval;
        name = xmlNodeListGetString (doc, key_nodes->nodeTab[0]->xmlChildrenNode, 1);
        
        key = xmlXPathEvalExpression((xmlChar *) "s3:Size", ctx);
        key_nodes = key->nodesetval;
        size = xmlNodeListGetString (doc, key_nodes->nodeTab[0]->xmlChildrenNode, 1);

        dir_tree_update_entry (dir_list->dir_tree, dir_list->path, name, atoll (size));

        xmlXPathFreeObject(key);
    }
    xmlXPathFreeObject (contents_xp);

    // directories
    subdirs_xp = xmlXPathEvalExpression ((xmlChar *) "//s3:CommonPrefixes", ctx);
    subdir_nodes = subdirs_xp->nodesetval;
    for(i = 0; i < subdir_nodes->nodeNr; i++) {
        ctx->node = subdir_nodes->nodeTab[i];

        // object name
        key = xmlXPathEvalExpression((xmlChar *) "s3:Prefix", ctx);
        key_nodes = key->nodesetval;

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

  doc = xmlReadMemory(xml, xml_len, "", NULL, 0);
  ctx = xmlXPathNewContext(doc);
  xmlXPathRegisterNs(ctx, (xmlChar *) "s3",
                     (xmlChar *) "http://s3.amazonaws.com/doc/2006-03-01/");
  marker_xp = xmlXPathEvalExpression((xmlChar *) "//s3:NextMarker", ctx);
  nodes = marker_xp->nodesetval;

  if(!nodes || nodes->nodeNr < 1) {
    next_marker = NULL;
  } else {
    next_marker = (char *) xmlNodeListGetString(doc, nodes->nodeTab[0]->xmlChildrenNode, 1);
  }

  xmlXPathFreeObject(marker_xp);
  xmlXPathFreeContext(ctx);
  xmlFreeDoc(doc);

  return next_marker;
}

// read callback function
static void s3http_connection_on_directory_listing (struct evhttp_request *req, void *ctx)
{   
    DirListRequest *dir_req = (DirListRequest *) ctx;
    struct evbuffer *inbuf;
    const char *buf;
    size_t buf_len;
    gchar *last_key;
    gchar *next_marker;
    gchar *req_path;
    int res;
    gchar *auth_str;
    struct evhttp_request *new_req;
    
    if (!req) {
        LOG_err ("Failed to get responce from server !");
        s3http_connection_get_directory_listing (dir_req->con, dir_req->path);
        return;
    }

    if (evhttp_request_get_response_code (req) != HTTP_OK) {
        LOG_err ("response code: %d", evhttp_request_get_response_code (req));
        return;
    }

    inbuf = evhttp_request_get_input_buffer (req);
    buf_len = evbuffer_get_length (inbuf);
    buf = (const char *) evbuffer_pullup (inbuf, buf_len);

    if (!buf_len) {
        // XXX: 
        dir_tree_stop_update (dir_req->dir_tree, dir_req->path);
        return;
    }
   
    g_printf ("======================\n%s\n=======================\n", buf);

    parse_dir_xml (dir_req, buf, buf_len);
    
    // repeat starting from the mark
    next_marker = get_next_marker (buf, buf_len);


    // check if we need to get more data
    if (!strstr (buf, "<IsTruncated>true</IsTruncated>") && !next_marker) {
        // we are done, stop updating
        dir_tree_stop_update (dir_req->dir_tree, dir_req->path);
        
        LOG_debug ("DONE !!");
        return;
    }

    // execute HTTP request
    auth_str = s3http_connection_get_auth_string (dir_req->con, "GET", "", dir_req->resource_path);
    req_path = g_strdup_printf ("%s?max-keys=%d&delimiter=/&marker=%s", dir_req->path, 2, next_marker);
    new_req = s3http_connection_create_request (dir_req->con, s3http_connection_on_directory_listing, dir_req, auth_str);
    res = evhttp_make_request (s3http_connection_get_evcon (dir_req->con), new_req, EVHTTP_REQ_GET, req_path);
    g_free (req_path);
}

// create DirListRequest
gboolean s3http_connection_get_directory_listing (S3HTTPConnection *con, const gchar *dir_path)
{
    DirListRequest *dir_req;
    struct evhttp_request *req;
    gchar *req_path;
    int res;
    gchar *auth_str;

    LOG_debug ("Getting directory listing for: %s", path);

    dir_req = g_new0 (DirListRequest, 1);
    dir_req->con = con;
    dir_req->app = s3http_connection_get_app (con);
    dir_req->dir_tree = application_get_dir_tree (dir_req->app);

    dir_req->resource_path = g_strdup_printf ("/%s%s", con->bucket_name, dir_path);
    dir_req->path = g_strdup (dir_path);
    auth_str = s3http_connection_get_auth_string (con, "GET", "", dir_req->resource_path);

    req_path = g_strdup_printf ("%s?max-keys=%d&delimiter=/&prefix=%s", dir_req->path, 100, "");

    // inform that we started to update the directory
    dir_tree_start_update (dir_req->dir_tree, path);

    req = s3http_connection_create_request (con, s3http_connection_on_directory_listing, dir_req, auth_str);
    res = evhttp_make_request (s3http_connection_get_evcon (con), req, EVHTTP_REQ_GET, req_path);
    g_free (req_path);
    g_free (auth_str);

    return TRUE;
}
