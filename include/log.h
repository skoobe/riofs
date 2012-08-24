#ifndef _H_LOG_H_
#define _H_LOG_H_

#include "include/global.h"

void logger_log_msg (const gchar *file, gint line, const gchar *func, 
        const gchar *format, ...);

#define LOG_debug(x...) \
G_STMT_START { \
    logger_log_msg (__FILE__, __LINE__, __func__, x); \
} G_STMT_END

#define LOG_msg(x...) \
G_STMT_START { \
    logger_log_msg (__FILE__, __LINE__, __func__, x); \
} G_STMT_END

#define LOG_err(x...) \
G_STMT_START { \
    logger_log_msg (__FILE__, __LINE__, __func__, x); \
} G_STMT_END

#endif
