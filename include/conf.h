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
void conf_list_set_string (ConfData *conf, const gchar *full_path, const gchar *val);

void conf_copy_entry (ConfData *dest, ConfData *src, const gchar *path, gboolean overwrite);
gboolean conf_node_exists (ConfData *conf, const gchar *path);

void conf_print (ConfData *conf);

typedef void (*ConfNodeChangeCB) (const gchar *path, gpointer user_data);
gboolean conf_set_node_change_cb (ConfData *conf, const gchar *path, ConfNodeChangeCB change_cb, gpointer user_data);

#endif
