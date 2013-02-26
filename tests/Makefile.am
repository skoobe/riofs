AM_CPPFLAGS = -I$(top_srcdir)/include
bin_PROGRAMS = s3http_client_test s3client_pool_test conf_test range_test cache_mng_test

s3http_client_test_SOURCES = $(top_srcdir)/src/s3http_client.c 
s3http_client_test_SOURCES += $(top_srcdir)/src/log.c
s3http_client_test_SOURCES += $(top_srcdir)/src/utils.c
s3http_client_test_SOURCES += s3http_client_test.c
s3http_client_test_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS)
s3http_client_test_LDADD = $(AM_LDADD) $(DEPS_LIBS)

s3client_pool_test_SOURCES = $(top_srcdir)/src/s3http_client.c
s3client_pool_test_SOURCES += $(top_srcdir)/src/s3client_pool.c 
s3client_pool_test_SOURCES += $(top_srcdir)/src/log.c
s3client_pool_test_SOURCES += test_application.c
s3client_pool_test_SOURCES += $(top_srcdir)/src/utils.c
s3client_pool_test_SOURCES += $(top_srcdir)/src/conf.c
s3client_pool_test_SOURCES += $(top_srcdir)/src/s3http_connection.c
s3client_pool_test_SOURCES += s3client_pool_test.c
s3client_pool_test_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS)
s3client_pool_test_LDADD = $(AM_LDADD) $(DEPS_LIBS)

conf_test_SOURCES = $(top_srcdir)/src/log.c
conf_test_SOURCES += $(top_srcdir)/src/utils.c
conf_test_SOURCES += $(top_srcdir)/src/conf.c
conf_test_SOURCES += conf_test.c
conf_test_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS)
conf_test_LDADD = $(AM_LDADD) $(DEPS_LIBS)

range_test_SOURCES = $(top_srcdir)/src/range.c
range_test_SOURCES += range_test.c
range_test_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS)
range_test_LDADD = $(AM_LDADD) $(DEPS_LIBS)

cache_mng_test_SOURCES = $(top_srcdir)/src/cache_mng.c
cache_mng_test_SOURCES += test_application.c
cache_mng_test_SOURCES += $(top_srcdir)/src/range.c
cache_mng_test_SOURCES += cache_mng_test.c
cache_mng_test_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS)
cache_mng_test_LDADD = $(AM_LDADD) $(DEPS_LIBS)
