AM_CPPFLAGS = -I$(top_srcdir)/include
bin_PROGRAMS = s3ffs
s3ffs_SOURCES = log.c
s3ffs_SOURCES += dir_tree.c  
s3ffs_SOURCES += s3fuse.c  
s3ffs_SOURCES += s3http_connection.c
s3ffs_SOURCES += s3http_connection_dir_list.c
s3ffs_SOURCES += s3http_connection_file_send.c
s3ffs_SOURCES += s3http_client.c
s3ffs_SOURCES += s3client_pool.c
s3ffs_SOURCES += main.c

s3ffs_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS)
s3ffs_LDADD = $(AM_LDADD) $(DEPS_LIBS)
