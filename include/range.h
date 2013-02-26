#ifndef _RANGE_H_
#define _RANGE_H_

#include "global.h"

typedef struct _Range Range;

Range *range_create ();

void range_destroy (Range *range);

void range_add (Range *range, guint64 start, guint64 end);

gboolean range_contain (Range *range, guint64 start, guint64 end);
gint range_count (Range *range);
guint64 range_length (Range *range);
void range_print (Range *range);

#endif
