#!/bin/sh
mkdir m4
autoreconf -fiv || exit 1;
echo "Now type './configure' to configure riofs project"
