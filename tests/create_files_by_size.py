import sys
import os
import time
import signal
import random
import shutil
import struct
import hashlib
import string

l_files = []

def str_gen (size=10, chars=string.ascii_uppercase + string.digits):
    return ''.join (random.choice(chars) for x in range(size))

def create_file (fname, flen):
    fout = open (fname, 'w')
    fout.write (os.urandom (flen))
    fout.close ()

def read_file (fname):
    fin = open (fname, 'r')
    x = fin.read ()
    fin.close ()

def create_files (dir, nr_files):
    for i in range (1, nr_files + 1):
        fname = dir + "/" + str_gen () + "_" + str (i)
        print "<<< " + fname
        create_file (fname, i)
        l_files.append (fname)

def read_files ():
    for l in l_files:
        print ">>> " + l
        read_file (l)

if __name__ == "__main__":
    if len (sys.argv) < 3:
        print "Please run as: " + sys.argv[0] + " [outdir] [file num]"
        sys.exit (1)

    create_files (sys.argv[1], int (sys.argv[2]))
    read_files ()
