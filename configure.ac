AC_PREREQ([2.59])
AC_INIT([s3ffs], [0.1])
AC_CANONICAL_TARGET
AM_INIT_AUTOMAKE 
AC_CONFIG_MACRO_DIR([m4])

AC_PROG_CC
AM_PROG_CC_C_O

AC_CONFIG_HEADERS([config.h]) 
PKG_CHECK_MODULES([DEPS], [glib-2.0 >= 2.32.1 fuse >= 2.8.4 libevent >= 2.1 libxml-2.0 >= 2.6 libcrypto >= 0.9])

CFLAGS="$CFLAGS -Wall -Wextra  -Wdeclaration-after-statement -Wredundant-decls -Wmissing-noreturn -Wshadow -Wpointer-arith -Wcast-align -Wwrite-strings -Winline -Wformat-nonliteral -Wformat-security -Wswitch-default -Winit-self -Wmissing-include-dirs -Wundef -Waggregate-return -Wmissing-format-attribute -Wnested-externs -Wstrict-overflow=5 -Wformat=2 -Wunreachable-code -Wfloat-equal -ffloat-store -g -ggdb3"

AC_CONFIG_FILES(Makefile src/Makefile tests/Makefile)
AC_OUTPUT()
