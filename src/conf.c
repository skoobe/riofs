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
#include "conf.h"

typedef enum {
    CT_NODE,
    CT_INT,
    CT_UINT,
    CT_STRING,
    CT_BOOLEAN,
    CT_LIST
} ConfType;

typedef struct _ConfNode ConfNode;

struct _ConfData {
    GHashTable *h_conf;
    GQueue *q_nodes;
};

struct _ConfNode {
    ConfType type;
    gchar *name;
    gchar *full_name;
    gpointer value;
    gboolean is_set;

    ConfNode *parent;

    ConfNodeChangeCB change_cb;
    gpointer user_data; // to pass to change_cb
};

static void conf_data_destroy (gpointer data);

#define CONF "conf"

ConfData *conf_create ()
{
    ConfData *conf;

    conf = g_new0 (ConfData, 1);
    conf->q_nodes =  g_queue_new ();
    conf->h_conf = g_hash_table_new_full (g_str_hash, g_str_equal, NULL, conf_data_destroy);

    return conf;
}

static void conf_data_destroy (gpointer data)
{
    ConfNode *conf_node = (ConfNode *) data;
    GList *l, *l_tmp;
    gchar *str;

    g_free (conf_node->name);
    g_free (conf_node->full_name);
    if (conf_node->type == CT_STRING) {
        str = (gchar *) conf_node->value;
        g_free (str);
    } else if (conf_node->type == CT_LIST) {
        l = (GList *) conf_node->value;
        for (l_tmp = g_list_first (l); l_tmp; l_tmp = l_tmp->next) {
            str = (gchar *) l_tmp->data;
            g_free (str);
        }
        g_list_free (l);
    }
    g_free (conf_node);
}

void conf_destroy (ConfData *conf)
{
    g_queue_free (conf->q_nodes);
    g_hash_table_destroy (conf->h_conf);

    g_free (conf);
}

static void start_element_handler (G_GNUC_UNUSED GMarkupParseContext *context, 
    const gchar *element_name, G_GNUC_UNUSED const gchar **attribute_names, 
    const gchar **attribute_values, G_GNUC_UNUSED gpointer user_data, G_GNUC_UNUSED GError **error)
{
    ConfData *conf = (ConfData *) user_data;
    ConfNode *conf_node;
    ConfNode *parent_node;

    if (!g_strcmp0 (element_name, "conf")) {
        return;
    }

    parent_node = g_queue_peek_head (conf->q_nodes);

    // create node and node's data
    conf_node = g_new0 (ConfNode, 1);
    conf_node->is_set = FALSE;

    if (parent_node) {
        if (parent_node->type != CT_NODE) {
            conf_node->parent = parent_node->parent;
        } else {
            conf_node->parent = parent_node;
        }
        conf_node->full_name = g_strdup_printf ("%s.%s", conf_node->parent->full_name, element_name);
    } else {
        conf_node->full_name = g_strdup (element_name);
        conf_node->parent = NULL;
    }
    conf_node->name = g_strdup (element_name);

    g_queue_push_head (conf->q_nodes, conf_node);

    if (!attribute_values[0]) {
        conf_node->type = CT_NODE;
        conf_node->value = NULL;
        conf_node->is_set = TRUE;
    } else if (!g_strcmp0 (attribute_values[0], "int")) {
        conf_node->type = CT_INT;
    } else if (!g_strcmp0 (attribute_values[0], "uint")) {
        conf_node->type = CT_UINT;
    } else if (!g_strcmp0 (attribute_values[0], "string")) {
        conf_node->type = CT_STRING;
    } else if (!g_strcmp0 (attribute_values[0], "boolean")) {
        conf_node->type = CT_BOOLEAN;
    } else if (!g_strcmp0 (attribute_values[0], "list")) {
        conf_node->type = CT_LIST;
    } else {
        LOG_err (CONF, "unknown config type: %s", attribute_values[0]);
        return;
    }
}

static void end_element_handler (G_GNUC_UNUSED GMarkupParseContext *context, G_GNUC_UNUSED const gchar *element_name, 
    G_GNUC_UNUSED gpointer user_data, G_GNUC_UNUSED GError **error)
{
    ConfData *conf = (ConfData *) user_data;
    ConfNode *conf_node;

    conf_node = g_queue_pop_head (conf->q_nodes);
    if (!conf_node)
        return;
    
    if (conf_node->type != CT_NODE)
        g_hash_table_insert (conf->h_conf, conf_node->full_name, conf_node);
    else 
        conf_data_destroy (conf_node);
}

static void text_handler (G_GNUC_UNUSED GMarkupParseContext *context, const gchar*text, 
    gsize text_len, G_GNUC_UNUSED gpointer user_data, G_GNUC_UNUSED GError **error)
{
    ConfData *conf = (ConfData *) user_data;
    gint32 tmp32;
    guint32 tmp_u32;
    ConfNode *conf_node;
    gchar *tmp_text;
    gchar *tok, *saved, *tok_striped;
    GList *l;

    conf_node = g_queue_peek_head (conf->q_nodes);
    if (!conf_node)
        return;

    if (conf_node->is_set)
        return;

    conf_node->is_set = TRUE;

    tmp_text = (gchar *) text;
    tmp_text[text_len] = '\0';

    if (conf_node->type == CT_INT) {
        tmp32 = atoi (tmp_text);
        conf_node->value = GINT_TO_POINTER (tmp32);
    } else if (conf_node->type == CT_UINT) {
        tmp_u32 = atoi (tmp_text);
        conf_node->value = GUINT_TO_POINTER (tmp_u32);
    } else if (conf_node->type == CT_STRING) {
        conf_node->value = g_strdup (tmp_text);
    } else if (conf_node->type == CT_BOOLEAN) {
        if (!strcasecmp (tmp_text, "true")) {
            conf_node->value = GINT_TO_POINTER (1);
        } else {
            conf_node->value = 0;
        }
    } else if (conf_node->type == CT_LIST) {
        l = NULL;
        // split string and remove whitespaces
        for (tok = strtok_r (tmp_text, ",", &saved); tok; tok = strtok_r (NULL, ",", &saved)) {
            tok_striped = g_strstrip (tok);
            if (tok_striped)
                l = g_list_append (l, g_strdup (tok_striped));
        }
        conf_node->value = l;
    } else {
        LOG_err (CONF, "unknown type");
    }
}

static void error_handler (G_GNUC_UNUSED GMarkupParseContext *context, GError *error, G_GNUC_UNUSED gpointer user_data)
{ 
    LOG_err (CONF, "Error parsing config file: %s", error->message);
}

/**
 * Parse config file 
 *
 * @retval TRUE if file is parsed
 * @retval FALSE if failed to parse file
 */
gboolean conf_parse_file (ConfData *conf, const gchar *filename)
{
    gchar *contents;
    gsize  length;
    GError *error;
    GMarkupParseContext *context;

    const GMarkupParser parser = {
        start_element_handler,
        end_element_handler,
        text_handler,
        NULL,
        error_handler
    };

    error = NULL;
    if (!g_file_get_contents (filename, &contents, &length, &error)) {
        LOG_err (CONF, error->message);
        g_error_free (error);
        return FALSE;
    }

    context = g_markup_parse_context_new (&parser, G_MARKUP_TREAT_CDATA_AS_TEXT, conf, NULL);

    if (!g_markup_parse_context_parse (context, contents, (gssize)length, NULL)) {
        g_markup_parse_context_free (context);
        return FALSE;
    }

    if (!g_markup_parse_context_end_parse (context, NULL)) {
        g_markup_parse_context_free (context);
        return FALSE;
    }

    g_markup_parse_context_free (context);
    g_free (contents);
    
    
    return TRUE;
}

/**
 * "section.name"
 */
const gchar *conf_get_string (ConfData *conf, const gchar *path)
{
    ConfNode *conf_node;

    conf_node = g_hash_table_lookup (conf->h_conf, path);
    if (!conf_node || conf_node->type != CT_STRING) {
        LOG_err (CONF, "Conf node not found: %s", path);
        return NULL;
    } else
        return (const gchar *) conf_node->value;
}

void conf_set_string (ConfData *conf, const gchar *full_path, const gchar *val)
{
    ConfNode *conf_node;

    conf_node = g_new0 (ConfNode, 1);
    conf_node->full_name = g_strdup (full_path);
    conf_node->name = g_strdup (full_path);
    conf_node->type = CT_STRING;
    conf_node->value = g_strdup (val);

    g_hash_table_replace (conf->h_conf, conf_node->full_name, conf_node);
}

gboolean conf_get_boolean (ConfData *conf, const gchar *path)
{
    ConfNode *conf_node;

    conf_node = g_hash_table_lookup (conf->h_conf, path);
    if (!conf_node || conf_node->type != CT_BOOLEAN) {
        LOG_err (CONF, "Conf node not found: %s", path);
        return FALSE;
    } else {
        if (conf_node->value)
            return TRUE;
        else
            return FALSE;
    }
}

void conf_set_boolean (ConfData *conf, const gchar *full_path, gboolean val)
{
    ConfNode *conf_node;

    conf_node = g_new0 (ConfNode, 1);
    conf_node->full_name = g_strdup (full_path);
    conf_node->name = g_strdup (full_path);
    conf_node->type = CT_BOOLEAN;
    conf_node->value = GINT_TO_POINTER (val);

    g_hash_table_replace (conf->h_conf, conf_node->full_name, conf_node);
}

gint32 conf_get_int (ConfData *conf, const gchar *path)
{
    ConfNode *conf_node;

    conf_node = g_hash_table_lookup (conf->h_conf, path);
    if (!conf_node || conf_node->type != CT_INT) {
        LOG_err (CONF, "Conf node not found: %s", path);
        return 0;
    } else
        return GPOINTER_TO_INT (conf_node->value);

}

void conf_set_int (ConfData *conf, const gchar *full_path, gint32 val)
{
    ConfNode *conf_node;

    conf_node = g_new0 (ConfNode, 1);
    conf_node->full_name = g_strdup (full_path);
    conf_node->name = g_strdup (full_path);
    conf_node->type = CT_INT;
    conf_node->value = GINT_TO_POINTER (val);

    g_hash_table_replace (conf->h_conf, conf_node->full_name, conf_node);
}


guint32 conf_get_uint (ConfData *conf, const gchar *path)
{
    ConfNode *conf_node;

    conf_node = g_hash_table_lookup (conf->h_conf, path);
    if (!conf_node || conf_node->type != CT_UINT) {
        LOG_err (CONF, "Conf node not found: %s", path);
        return 0;
    } else
        return GPOINTER_TO_UINT (conf_node->value);
}

void conf_set_uint (ConfData *conf, const gchar *full_path, guint32 val)
{
    ConfNode *conf_node;

    conf_node = g_new0 (ConfNode, 1);
    conf_node->full_name = g_strdup (full_path);
    conf_node->name = g_strdup (full_path);
    conf_node->type = CT_UINT;
    conf_node->value = GUINT_TO_POINTER (val);

    g_hash_table_replace (conf->h_conf, conf_node->full_name, conf_node);
}

GList *conf_get_list (ConfData *conf, const gchar *path)
{
    ConfNode *conf_node;

    conf_node = g_hash_table_lookup (conf->h_conf, path);
    if (!conf_node || conf_node->type != CT_LIST) {
        LOG_err (CONF, "Conf node not found: %s", path);
        return 0;
    } else
        return (GList *) conf_node->value;
}

void conf_list_set_string (ConfData *conf, const gchar *full_path, const gchar *val)
{
    ConfNode *conf_node;
    GList *l;

    conf_node = g_hash_table_lookup (conf->h_conf, full_path);
    if (conf_node) {
        l = (GList *) conf_node->value;
        l = g_list_append (l, g_strdup (val));
        conf_node->value = (gpointer) l;
    } else {
        l = NULL;
        l = g_list_append (l, g_strdup (val));

        conf_node = g_new0 (ConfNode, 1);
        conf_node->full_name = g_strdup (full_path);
        conf_node->name = g_strdup (full_path);
        conf_node->type = CT_LIST;
        conf_node->value = l;
        g_hash_table_replace (conf->h_conf, conf_node->full_name, conf_node);
    }
}

static void conf_node_print (G_GNUC_UNUSED gpointer key, gpointer value, G_GNUC_UNUSED gpointer user_data)
{
    ConfNode *conf_node = (ConfNode *) value;
    GList *l, *l_tmp;
    gchar *str;

    g_printf ("%s", conf_node->full_name);
    if (conf_node->type == CT_NODE) {
        g_printf (":\n");
    } else if (conf_node->type == CT_INT) {
        g_printf (" = %i\n", GPOINTER_TO_INT (conf_node->value));
    } else if (conf_node->type == CT_STRING) {
        g_printf (" = %s\n", (gchar *)conf_node->value);
    } else if (conf_node->type == CT_BOOLEAN && conf_node->value) {
        g_printf (" = TRUE\n");
    } else if (conf_node->type == CT_BOOLEAN && !conf_node->value) {
        g_printf (" = FALSE\n");
    } else if (conf_node->type == CT_LIST) {
        l = (GList *) conf_node->value;
        g_printf (" = ");
        for (l_tmp = g_list_first (l); l_tmp; l_tmp = l_tmp->next) {
            str = (gchar *) l_tmp->data;
            g_printf ("%s ", str);
        }
        g_printf ("\n");
    } else {
        g_printf (" = UNKNOWN\n");
    }
}
/**
 * Prints content of config file to stdout
 */
void conf_print (ConfData *conf)
{
    g_printf ("=============== \n");
    g_hash_table_foreach (conf->h_conf, conf_node_print, NULL);
    g_printf ("=============== \n");
}

gboolean conf_set_node_change_cb (ConfData *conf, const gchar *path, ConfNodeChangeCB change_cb, gpointer user_data)
{
    ConfNode *conf_node;

    conf_node = g_hash_table_lookup (conf->h_conf, path);
    if (!conf_node) {
        LOG_err (CONF, "Conf node %s not found !", path);
        return FALSE;
    }

    conf_node->change_cb = change_cb;
    conf_node->user_data = user_data;

    return TRUE;
}
