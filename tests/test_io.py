#!/usr/bin/env python2

from subprocess import Popen, PIPE
from StringIO import StringIO
import random
import string
import hashlib

dst="/tmp/aa/"

def upload(filename, content):
    s = dst + filename;
    f = open(s, 'w')
    f.write (content);
    f.close ();

def download(filename):
    s = dst + filename;
    f = open(s, 'r')
    content = f.read ();
    f.close ();
    return content

def random_string(length):
    s = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))
    checksum = hashlib.sha256(s).hexdigest()
    return checksum + "_" + s


i = 0
while True:
    i += 1
    key = "test_%d" % random.randint(0, 1000000)
    
    s1 = '%030x' % random.randrange(256**15)
    upload(key, s1)
    s2 = download(key)
    print ("Writing: ", key);

    if s1 != s2:
        print "length mismatch: len(s1)=%d len(s2)=%d" % (len(s1), len(s2))
