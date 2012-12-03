AC_INIT([s3ffs], [0.1])
AC_CONFIG_MACRO_DIR([m4])
AC_CANONICAL_TARGET
AM_INIT_AUTOMAKE 
AC_CONFIG_HEADERS([include/config.h]) 

AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_RANLIB

PKG_CHECK_MODULES([DEPS], [glib-2.0 >= 2.32.1 fuse >= 2.8.4 libevent >= 2.0 libxml-2.0 >= 2.6 libcrypto >= 0.9])

CFLAGS="$CFLAGS -Wall -Wextra -march=native -O1 -Wdeclaration-after-statement -Wredundant-decls -Wmissing-noreturn -Wshadow -Wpointer-arith -Wcast-align -Wwrite-strings -Winline -Wformat-nonliteral -Wformat-security -Wswitch-default -Winit-self -Wmissing-include-dirs -Wundef -Waggregate-return -Wmissing-format-attribute -Wnested-externs -Wstrict-overflow=5 -Wformat=2 -Wunreachable-code -Wfloat-equal -ffloat-store -g -ggdb3"

AC_CONFIG_FILES(Makefile src/Makefile include/Makefile tests/Makefile)
AC_OUTPUT
