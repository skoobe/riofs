AM_CPPFLAGS = -I$(top_srcdir)/include
bin_PROGRAMS = s3ffs
s3ffs_SOURCES = bucket_connection.c  cache_mng.c  connection_pool.c  dir_tree.c  fuse.c  log.c  main.c  s3_dir_list.c  s3_file_get.c  s3_file_put.c  s3_http_client.c
s3ffs_CFLAGS = $(AM_CFLAGS) $(DEPS_CFLAGS)
s3ffs_LDADD = $(AM_LDADD) $(DEPS_LIBS)
