AM_CPPFLAGS = -I$(top_srcdir)/include
bin_PROGRAMS = s3connection_test
s3connection_test_SOURCES = s3connection_test.c $(top_srcdir)/src/s3_http_client.c $(top_srcdir)/src/log.c
s3connection_test_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS)
s3connection_test_LDADD = $(AM_LDADD) $(DEPS_LIBS)
