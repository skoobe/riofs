import os
import sys
import time
import threading

class DirList (threading.Thread):
    def __init__ (self, path):
        threading.Thread.__init__ (self)
        self.path = path
        self.items = 0

    def run (self):
        l = os.listdir (self.path)
        self.items = len (l)

    def join (self):
        threading.Thread.join (self)
        return self.items

def main():
    if len(sys.argv) < 3:
        sys.exit("Usage: {} [num threads] [path]".format(sys.argv[0]))
 
    t_list = []

    for i in range (0, int (sys.argv[1])):
        t = DirList (sys.argv[2])
        t.start ()
        t_list.append (t)
        time.sleep (1)


    l_res = []
    for t in t_list:
        l_res.append (t.join ())

    res = -1
    for r in l_res:
        if res == -1:
            res = r

        if res != r:
            print "Failed: " + str (res) + " != " + str (r)
            sys.exit (1)
            
    print "Test passed !"


if __name__ == "__main__":
    main()
