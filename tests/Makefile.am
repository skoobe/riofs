AM_CPPFLAGS = -I$(top_srcdir)/include
if BUILD_TEST_APPS
bin_PROGRAMS = client_pool_test conf_test range_test cache_mng_test
endif
EXTRA_DIST = test.conf.xml

client_pool_test_SOURCES = $(top_srcdir)/src/client_pool.c 
client_pool_test_SOURCES += $(top_srcdir)/src/log.c
client_pool_test_SOURCES += test_application.c
client_pool_test_SOURCES += $(top_srcdir)/src/utils.c
client_pool_test_SOURCES += $(top_srcdir)/src/conf.c
client_pool_test_SOURCES += $(top_srcdir)/src/http_connection.c
client_pool_test_SOURCES += client_pool_test.c
client_pool_test_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS) $(LIBEVENT_OPENSSL_CFLAGS) $(SSL_CFLAGS)
client_pool_test_LDADD = $(AM_LDADD) $(DEPS_LIBS) $(LIBEVENT_OPENSSL_LIBS) $(SSL_LIBS)

conf_test_SOURCES = $(top_srcdir)/src/log.c
conf_test_SOURCES += $(top_srcdir)/src/utils.c
conf_test_SOURCES += $(top_srcdir)/src/conf.c
conf_test_SOURCES += conf_test.c
conf_test_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS) $(LIBEVENT_OPENSSL_CFLAGS) $(SSL_CFLAGS)
conf_test_LDADD = $(AM_LDADD) $(DEPS_LIBS) $(LIBEVENT_OPENSSL_LIBS) $(SSL_LIBS)

range_test_SOURCES = $(top_srcdir)/src/range.c
range_test_SOURCES += range_test.c
range_test_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS) $(LIBEVENT_OPENSSL_CFLAGS) $(SSL_CFLAGS)
range_test_LDADD = $(AM_LDADD) $(DEPS_LIBS) $(LIBEVENT_OPENSSL_LIBS) $(SSL_LIBS)

cache_mng_test_SOURCES = $(top_srcdir)/src/cache_mng.c
cache_mng_test_SOURCES += $(top_srcdir)/src/range.c
cache_mng_test_SOURCES += $(top_srcdir)/src/utils.c
cache_mng_test_SOURCES += $(top_srcdir)/src/conf.c
cache_mng_test_SOURCES += $(top_srcdir)/src/log.c
cache_mng_test_SOURCES += test_application.c
cache_mng_test_SOURCES += cache_mng_test.c
cache_mng_test_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS) $(LIBEVENT_OPENSSL_CFLAGS) $(SSL_CFLAGS)
cache_mng_test_LDADD = $(AM_LDADD) $(DEPS_LIBS) $(LIBEVENT_OPENSSL_LIBS) $(SSL_LIBS)
