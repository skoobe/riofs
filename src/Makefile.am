AM_CPPFLAGS = -I$(top_srcdir)/include -DSYSCONFDIR=\""$(sysconfdir)/@PACKAGE@/"\" 
bin_PROGRAMS = riofs
riofs_SOURCES = log.c
riofs_SOURCES += dir_tree.c  
riofs_SOURCES += rfuse.c  
riofs_SOURCES += http_connection.c
riofs_SOURCES += http_connection_dir_list.c
riofs_SOURCES += bucket_client.c
riofs_SOURCES += client_pool.c
riofs_SOURCES += file_io_ops.c
riofs_SOURCES += cache_mng.c
riofs_SOURCES += stat_srv.c
riofs_SOURCES += utils.c
riofs_SOURCES += conf.c
riofs_SOURCES += range.c
riofs_SOURCES += main.c

riofs_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS)
riofs_LDADD = $(AM_LDADD) $(DEPS_LIBS)
