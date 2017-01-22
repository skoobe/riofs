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
#ifndef _H_LOG_H_
#define _H_LOG_H_

typedef enum {
    LOG_err = 0,
    LOG_msg = 1,
    LOG_debug = 2,
} LogLevel;

#include "global.h"

void logger_log_msg (const gchar *file, gint line, const gchar *func,
        LogLevel level, const gchar *subsystem,
        const gchar *format, ...) __attribute__((format(printf, 6, 7)));

void logger_set_syslog (gboolean use);
void logger_set_color (gboolean use);
void logger_set_file (FILE *f);
void logger_destroy (void);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wvariadic-macros"

#define LOG_debug(subsystem, x...) \
G_STMT_START { \
    logger_log_msg (__FILE__, __LINE__, __func__, LOG_debug, subsystem, x); \
} G_STMT_END

#define LOG_msg(subsystem, x...) \
G_STMT_START { \
    logger_log_msg (__FILE__, __LINE__, __func__, LOG_msg, subsystem, x); \
} G_STMT_END

#define LOG_err(subsystem, x...) \
G_STMT_START { \
    logger_log_msg (__FILE__, __LINE__, __func__, LOG_err, subsystem, x); \
} G_STMT_END

#pragma GCC diagnostic pop

#endif
