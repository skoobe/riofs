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
#include "log.h"
#include <syslog.h>

static gboolean use_syslog = FALSE;
static gboolean use_color = FALSE;

typedef struct {
    gint id;
    const gchar *prefix;
} ColorData;

typedef enum {
// CD_RED = 0, reserved for ERR
    CD_GREEN = 0,
    CD_YELLOW = 1,
    CD_BLUE = 2,
    CD_MAGENTA = 3,
    CD_CYAN = 4,
    CD_DEFAULT
} ColorDef;

// <ESC>[{attr};{fg};{bg}m
static const gchar *colors[] = {
//    "\033[31m",  CD_RED
    "\033[32m",
    "\033[33m",
    "\033[34m",
    "\033[35m",
    "\033[36m",
};

static const gchar c_reset[] = "\033[0m";
static FILE *f_log = NULL;

// prints a message string to stdout
// XXX: extend it (syslog, etc)
void logger_log_msg (G_GNUC_UNUSED const gchar *file, G_GNUC_UNUSED gint line, G_GNUC_UNUSED const gchar *func, 
        LogLevel level, const gchar *subsystem,
        const gchar *format, ...)
{
    va_list args;
    char out_str[1024];
    struct tm cur;
    char ts[50];
    time_t t;
    struct tm *cur_p;

    if (log_level < level)
        return;

    t = time (NULL);
    localtime_r (&t, &cur);
    cur_p = &cur;
    if (!strftime (ts, sizeof (ts), "%H:%M:%S", cur_p)) {
        ts[0] = '\0';
    }

    va_start (args, format);
        g_vsnprintf (out_str, sizeof (out_str), format, args);
    va_end (args);

    if (!f_log)
        f_log = stdout;

    if (log_level == LOG_debug) {
        if (level == LOG_err) {
            if (use_color) {
                g_fprintf (f_log, "%s \033[1;31m[%s]\033[0m  (%s %s:%d) \033[1;31m%s\033[0m\n", ts, subsystem, func, file, line, out_str);
            } else {
                g_fprintf (f_log, "%s \033[1;31m[%s]\033[0m  (%s %s:%d) %s\n", ts, subsystem, func, file, line, out_str);
            }
        } else {
            if (use_color) {
                guint i;
                i = g_str_hash (subsystem) % CD_DEFAULT;
                g_fprintf (f_log, "%s [%s%s%s] (%s %s:%d) %s%s%s\n", ts, colors[i], subsystem, c_reset, func, file, line, colors[i], out_str, c_reset);
            } else {
                g_fprintf (f_log, "%s [%s] (%s %s:%d) %s\n", ts, subsystem, func, file, line, out_str);
            }
        }
    }
    else {
        if (use_syslog)
            syslog (log_level == LOG_msg ? LOG_INFO : LOG_ERR, "%s", out_str);
        else {
            if (level == LOG_err)
                g_fprintf (f_log, "\033[1;31mERROR!\033[0m %s\n", out_str);
            else
                g_fprintf (f_log, "%s\n", out_str);
        }
    }
}

void logger_destroy (void)
{
    if (use_syslog)
        closelog ();
}

void logger_set_syslog (gboolean use)
{
    use_syslog = use;
}

void logger_set_color (gboolean use)
{
    use_color = use;
}

void logger_set_file (FILE *f)
{
    f_log = f;
}
