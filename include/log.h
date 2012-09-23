#ifndef _H_LOG_H_
#define _H_LOG_H_

#include "include/global.h"

typedef enum {
    LOG_err = 0,
    LOG_msg = 1,
    LOG_debug = 2,
} LogLevel;

void logger_log_msg (const gchar *file, gint line, const gchar *func,
        LogLevel level, const gchar *subsystem,
        const gchar *format, ...);

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

#endif
