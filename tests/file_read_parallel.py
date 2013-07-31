import sys
import os
from threading import Thread, Lock
import random
import time
from os.path import isfile, join

class FileReadThread (Thread):
    def __init__ (self, lock, items):
        Thread.__init__ (self)
        self.failed = False
        self.items = items
        self.lock = lock
        random.shuffle (self.items)

    def run (self):
        for fname in self.items:
            try:
                fin = open (fname, 'r')
                fin.read ()
                fin.close ()
            except Exception, e:
                self.lock.acquire ()
                print "Exception: " + str (e)
                self.lock.release ()
                self.failed = True
                break

    def join (self):
        Thread.join (self)
        return self.failed

def main (num_threads, path):
    # get files only
    l = [ f for f in os.listdir (path) if isfile (join (path, f)) ]
    failed = False

    lock = Lock ()

    t_list = []
    for i in range (0, num_threads):
        # send a copy
        t = FileReadThread (lock, l[:])
        t.start ()
        t_list.append (t)
    
    res_list = []
    for t in t_list:
        failed = failed or t.join ()

    
    time.sleep (1)
    if failed:
        print "Test FAILED !"
        sys.exit (1)
            
    print "Test passed !"

if __name__ == "__main__":
    if len (sys.argv) < 3:
        sys.exit("Usage: {} [num threads] [path]".format(sys.argv[0]))
        sys.exit (1)
    
    main (int (sys.argv[1]), sys.argv[2])
