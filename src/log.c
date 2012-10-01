#include "include/log.h"

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
	gmtime_r (&t, &cur);
	cur_p = &cur;
    if (!strftime (ts, sizeof (ts), "%H:%M:%S", cur_p)) {
        ts[0] = '\0';
    }

    va_start (args, format);
        g_vsnprintf (out_str, sizeof (out_str), format, args);
    va_end (args);

//#ifdef DEBUG
    g_fprintf (stdout, "%s %s %s:%d \t%s\n", ts, func, file, line, out_str);
//#else
//    g_fprintf (stdout, "[%s][%s]\t%s\n", ts, subsystem, out_str);
//#endif
}
