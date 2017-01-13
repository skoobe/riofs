m4_define([version_major], [0])
m4_define([version_minor], [7])
m4_define([version_micro], [0])
m4_define([version_micro_extra], version_micro)
m4_append([version_micro_extra], [])

AC_INIT([riofs], [version_major.version_minor.version_micro_extra], [paul.ionkin@gmail.com], , [https://github.com/skoobe/riofs])

AC_PREREQ(2.61)
AC_CONFIG_MACRO_DIR([m4])
AC_CANONICAL_TARGET
AM_INIT_AUTOMAKE([1.9 tar-ustar dist-bzip2 no-dist-gzip subdir-objects no-dependencies])

AC_DEFINE(VERSION_MAJOR, version_major, [RioFS major version])
AC_DEFINE(VERSION_MINOR, version_minor, [RioFS minor version])
AC_DEFINE(VERSION_MICRO, version_micro, [RioFS micro version])

AC_CONFIG_HEADERS([include/config.h])

AC_USE_SYSTEM_EXTENSIONS

AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_INSTALL
AC_PROG_MKDIR_P
AC_PROG_LN_S
AC_PROG_RANLIB
PKG_PROG_PKG_CONFIG
AC_CHECK_SIZEOF(long int)

AC_SYS_LARGEFILE
AC_FUNC_FSEEKO
AC_TYPE_OFF_T

AC_STRUCT_TM
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_TYPE_PID_T

PKG_CHECK_MODULES([DEPS], [glib-2.0 >= 2.22 fuse >= 2.7.3 libxml-2.0 >= 2.6 libcrypto >= 0.9])

AC_ARG_WITH(libevent,
    AS_HELP_STRING(--with-libevent=PATH, base of libevent2 installation),
    [
    case $with_libevent in
    no)
        AC_MSG_ERROR([Please specify libevent2 installation path])
    ;;
    yes)
        PKG_CHECK_MODULES([LEDEPS], [libevent >= 2.0])
    ;;
    *)
        CFLAGS="-I$with_libevent/include"
        LDFLAGS="-L$with_libevent/lib -levent"

        AC_CHECK_HEADERS(event2/event.h, [],
            AC_MSG_ERROR([libevent2 headers not found or not usable])
        )

        AC_CHECK_LIB(event, [main], [],
            AC_MSG_ERROR([Could not find libevent2])
        )

        LEDEPS_CFLAGS="-I$with_libevent/include"
        LEDEPS_LIBS="-L$with_libevent/lib -levent"
    ;;
    esac
    ],
    [
        PKG_CHECK_MODULES([LEDEPS], [libevent >= 2.0])
    ]
)

# check if we should link against libevent_openssl
AC_ARG_ENABLE(openssl,
    AS_HELP_STRING(--disable-openssl, disable support for openssl encryption (libevent_openssl must be installed)),
    [], [enable_openssl=yes]
)

if test "x$enable_openssl" = "xyes" ; then
    # check if openssl is present
    AC_CHECK_LIB([ssl], [SSL_library_init], [], [AC_MSG_FAILURE([could not find OpenSSL])])
    PKG_CHECK_MODULES([LIBEVENT_OPENSSL], [libevent_openssl >= 2.1.2], [found_libevent_openssl=yes], [found_libevent_openssl=no])
    if test "x$found_libevent_openssl" = "xyes" ; then
        AC_DEFINE([SSL_ENABLED], [1], [Define to 1 if SSL support is enabled])
    fi
fi

#AC_MSG_CHECKING(for libmagic)
libmagic=false
AC_ARG_WITH(libmagic,
  AS_HELP_STRING(--with-libmagic=PATH, base of libmagic installation),
  [AC_MSG_RESULT($with_libmagic)
   case $with_libmagic in
   no)
    libmagic=false
     ;;
   yes)
    AC_CHECK_HEADERS([magic.h],, [
        AC_MSG_ERROR([magic.h header not found or not usable])
    ])
    AC_CHECK_LIB(magic, [magic_open], [MAGIC_LIBS="-lmagic"],[
            AC_MSG_ERROR([Could not find libmagic])
    ])
    libmagic=true
     ;;
   *)
    # set temp variables
    LDFLAGS="-L$with_libmagic/lib $LDFLAGS"
    CFLAGS="-I$with_libmagic/include $CFLAGS"

    AC_CHECK_HEADERS(magic.h,
     EXT_LIB_PATH="-L$with_libmagic/lib $EXT_LIB_PATH"
     MAGIC_CFLAGS="-L$with_libmagic/lib"
     MAGIC_LDFLAGS="-I$with_libmagic/include",
     AC_MSG_ERROR([magic.h header not found or not usable])
    )
    AC_CHECK_LIB(magic, [magic_open], [MAGIC_LIBS="-lmagic"],[
            AC_MSG_ERROR([Could not find libmagic])
    ])

    libmagic=true

    #restore
    LDFLAGS=$SAVE_LDFLAGS
    CFLAGS=$SAVE_CFLAGS
    ;;
   esac
  ],
  [
    libmagic=true
    AC_CHECK_HEADERS([magic.h],, [
        libmagic=false
    ])
    AC_CHECK_LIB(magic, [magic_open], [MAGIC_LIBS="-lmagic"],[
        libmagic=false
    ])
  ])
AC_SUBST(MAGIC_CFLAGS)
AC_SUBST(MAGIC_LDFLAGS)
AC_SUBST(MAGIC_LIBS)
if test "x$libmagic" = "xtrue" ; then
    AC_DEFINE(MAGIC_ENABLED, [1], [Define to use libmagic])
fi

AX_EXECINFO()

# check if we need to build test applications
AC_MSG_CHECKING([if building example applications])
AC_ARG_ENABLE([test-apps],
     AS_HELP_STRING(--enable-test-apps, build test applications),
	[], [enable_test_apps=no]
)
AM_CONDITIONAL([BUILD_TEST_APPS], [test "$enable_test_apps" = "yes"])
AC_MSG_RESULT([$enable_test_apps])

AC_DEFINE(FUSE_USE_VERSION, 26, [Fuse API Version])

# check if we should enable strict compile warnings
AC_ARG_ENABLE(strict-compile,
     AS_HELP_STRING(--enable-strict-compile, enable support for strict compiler warnings),
     [], [enable_strict_compile=no]
)

if test "x$enable_strict_compile" = "xyes" ; then
    CFLAGS="$CFLAGS -Wextra -pedantic -Wall"
fi

# check if we should enable verbose debugging
AC_ARG_ENABLE(debug,
     AS_HELP_STRING(--enable-debug, enable support for running in debug mode),
     [], [enable_debug=no]
)

if test "x$enable_debug" = "xyes" ; then
    CFLAGS="$CFLAGS -Wall -Wextra -O0 -Wdeclaration-after-statement -Wredundant-decls -Wmissing-noreturn -Wshadow -Wpointer-arith -Wcast-align -Wwrite-strings -Winline -Wformat-nonliteral -Wformat-security -Wswitch-default -Winit-self -Wmissing-include-dirs -Wundef -Waggregate-return -Wmissing-format-attribute -Wnested-externs -Wstrict-overflow=5 -Wformat=2 -Wunreachable-code -Wfloat-equal -ffloat-store -g -ggdb3"
fi

AC_CONFIG_FILES(Makefile src/Makefile include/Makefile tests/Makefile)
AC_OUTPUT
