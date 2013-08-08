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
#ifndef _CONF_KEYS_H_
#define _CONF_KEYS_H_

#include "global.h"

const gchar *conf_keys_str[] = {
    "app.foreground",
    "log.use_syslog",
    "log.use_color",
    "log.level",
    "pool.writers",
    "pool.readers",
    "pool.operations",
    "pool.max_requests_per_pool",
    "s3.path_style",
    "s3.keys_per_request",
    "s3.part_size",
    "s3.check_empty_files",
    "s3.storage_type",
    "connection.timeout",
    "connection.retries",
    "connection.max_redirects",
    "connection.max_retries",
    "filesystem.dir_cache_max_time",
    "filesystem.md5_enabled",
    "filesystem.cache_enabled",
    "filesystem.cache_dir",
    "filesystem.cache_dir_max_size",
    "filesystem.cache_object_ttl",
    "statistics.enabled",
    "statistics.host",
    "statistics.port",
    "statistics.stats_path",
    "statistics.history_size",
    "statistics.access_key"
};

guint conf_keys_len = sizeof (conf_keys_str) / sizeof (conf_keys_str[0]);

#endif
