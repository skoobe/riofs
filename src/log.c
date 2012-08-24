#include "include/log.h"

// prints a message string to stdout
void logger_log_msg (G_GNUC_UNUSED const gchar *file, G_GNUC_UNUSED gint line, G_GNUC_UNUSED const gchar *func, 
        const gchar *format, ...)
{
    va_list args;
    char out_str[1024];

    va_start (args, format);
        g_vsnprintf (out_str, 1024, format, args);
    va_end (args);

#ifdef DEBUG
    g_fprintf (stdout, "%s:%d(%s) %s\n", file, line, func, out_str);
#else
    g_fprintf (stdout, "%s\n",out_str);
#endif
}
