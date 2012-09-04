#ifndef _FUSE_H_
#define _FUSE_H_

#include "include/global.h"

struct dirbuf {
	char *p;
	size_t size;
};

S3Fuse *s3fuse_create (Application *app, int argc, char *argv[]);

void s3fuse_add_dirbuf (fuse_req_t req, struct dirbuf *b, const char *name, fuse_ino_t ino);

#endif
