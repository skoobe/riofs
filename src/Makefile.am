AM_CPPFLAGS = -I$(top_srcdir)/include
bin_PROGRAMS = s3ffs
s3ffs_SOURCES = log.c
s3ffs_SOURCES += bucket_connection.c
s3ffs_SOURCES += cache_mng.c
s3ffs_SOURCES += dir_tree.c  
s3ffs_SOURCES += s3_fuse.c  
s3ffs_SOURCES += s3_dir_list.c
s3ffs_SOURCES += s3_file_get.c
s3ffs_SOURCES += s3_file_put.c
s3ffs_SOURCES += s3_http_client.c
s3ffs_SOURCES += s3_http_client_pool.c
s3ffs_SOURCES += main.c

s3ffs_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS)
s3ffs_LDADD = $(AM_LDADD) $(DEPS_LIBS)
