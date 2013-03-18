AC_PREREQ(2.61)
AC_INIT([riofs], [0.2], [paul.ionkin@gmail.com])
AC_CONFIG_MACRO_DIR([m4])
AC_CANONICAL_TARGET
AM_INIT_AUTOMAKE 
AC_CONFIG_HEADERS([include/config.h]) 

AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_RANLIB
PKG_PROG_PKG_CONFIG

PKG_CHECK_MODULES([DEPS], [glib-2.0 >= 2.32.1 fuse >= 2.8.4 libevent >= 2.0 libxml-2.0 >= 2.6 libcrypto >= 0.9])

# check if we should link against libevent_openssl
AC_ARG_ENABLE(openssl,
     AS_HELP_STRING(--disable-openssl, disable support for openssl encryption (libevent_openssl must be installed)),
        [], [enable_openssl=yes])
if test x$enable_openssl = xyes; then
    # check if openssl is present
    AC_CHECK_LIB([ssl], [SSL_library_init], [], [AC_MSG_FAILURE([could not find OpenSSL])])
    PKG_CHECK_MODULES([LIBEVENT_OPENSSL], [libevent_openssl >= 2.1.2], [found_libevent_openssl=yes], [found_libevent_openssl=no])
    if test x$found_libevent_openssl = xyes; then
        AC_DEFINE([SSL_ENABLED], [1], [Define to 1 if SSL support is enabled])
    fi
fi

# check if we need to build test applications
AC_ARG_ENABLE([test-apps],
     AS_HELP_STRING([--enable-test-apps build test applications]),
	[], [enable_test_apps=no])
AM_CONDITIONAL([BUILD_TEST_APPS], [test "$enable_test_apps" = "yes"])

# check if we should enable verbose debugging 
AC_ARG_ENABLE(debug-mode,
     AS_HELP_STRING(--enable-debug-mode, enable support for running in debug mode),
        [], [enable_debug_mode=no])
if test x$enable_debug_mode = xyes; then
    CFLAGS="$CFLAGS -Wall -Wextra -march=native -O1 -Wdeclaration-after-statement -Wredundant-decls -Wmissing-noreturn -Wshadow -Wpointer-arith -Wcast-align -Wwrite-strings -Winline -Wformat-nonliteral -Wformat-security -Wswitch-default -Winit-self -Wmissing-include-dirs -Wundef -Waggregate-return -Wmissing-format-attribute -Wnested-externs -Wstrict-overflow=5 -Wformat=2 -Wunreachable-code -Wfloat-equal -ffloat-store -g -ggdb3  -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE"
else
    CFLAGS="-D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -O2 -march=native"
fi

AC_CONFIG_FILES(Makefile src/Makefile include/Makefile tests/Makefile)
AC_OUTPUT
