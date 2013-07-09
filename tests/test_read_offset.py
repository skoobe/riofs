import sys
import os
import time
import signal
import random
import shutil
import struct
import hashlib
import string

def read_offset (fname, offset):
    o = 0
    fsize = os.path.getsize (fname)
    fin = open (fname, 'r')
    run = 1
    while run:
        fin.seek (o);
        x = fin.read (offset)
        if len (x) == 0:
            break
        o = o + len (x)
        print "Read: " + str (len (x)) + " So far: " + str (o) + " File size: " + str (fsize)
    fin.close ()

if __name__ == "__main__":
    if len (sys.argv) < 3:
        print "Please run as: " + sys.argv[0] + " [filename] [offset step]"
        sys.exit (1)

    read_offset (sys.argv[1], int (sys.argv[2]))
