AM_CPPFLAGS = -I$(top_srcdir)/include
bin_PROGRAMS = s3http_client_test s3http_client_pool_test

s3http_client_test_SOURCES = $(top_srcdir)/src/s3http_client.c $(top_srcdir)/src/log.c
s3http_client_test_SOURCES += s3http_client_test.c
s3http_client_test_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS)
s3http_client_test_LDADD = $(AM_LDADD) $(DEPS_LIBS)

s3http_client_pool_test_SOURCES = $(top_srcdir)/src/s3http_client.c $(top_srcdir)/src/s3http_client_pool.c $(top_srcdir)/src/log.c
s3http_client_pool_test_SOURCES += s3http_client_pool_test.c
s3http_client_pool_test_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS)
s3http_client_pool_test_LDADD = $(AM_LDADD) $(DEPS_LIBS)
