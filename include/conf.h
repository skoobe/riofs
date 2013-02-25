/* Copyright (C) 2012-2013 Paul Ionkin <paul.ionkin@gmail.com>
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 */
#ifndef _CONF_PARSER_H_
#define _CONF_PARSER_H_

#include "global.h"

ConfData *conf_create ();
void conf_destroy ();

gboolean conf_parse_file (ConfData *conf, const gchar *filename);

const gchar *conf_get_string (ConfData *conf, const gchar *path);
void conf_set_string (ConfData *conf, const gchar *full_path, const gchar *val);

gint32 conf_get_int (ConfData *conf, const gchar *path);
void conf_set_int (ConfData *conf, const gchar *full_path, gint32 val);

guint32 conf_get_uint (ConfData *conf, const gchar *path);
void conf_set_uint (ConfData *conf, const gchar *full_path, guint32 val);

gboolean conf_get_boolean (ConfData *conf, const gchar *path);
void conf_set_boolean (ConfData *conf, const gchar *full_path, gboolean val);

GList *conf_get_list (ConfData *conf, const gchar *path);
void conf_list_add_string (ConfData *conf, const gchar *full_path, const gchar *val);

void conf_print (ConfData *conf);

typedef void (*ConfNodeChangeCB) (const gchar *path, gpointer user_data);
gboolean conf_set_node_change_cb (ConfData *conf, const gchar *path, ConfNodeChangeCB change_cb, gpointer user_data);

#endif
