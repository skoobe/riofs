#ifndef _CACHE_MNG_H_
#define _CACHE_MNG_H_

#include "include/global.h"

CacheMng *cache_mng_create (Application *app, const gchar *path);
void cache_mng_destroy (CacheMng *mng);

#endif
