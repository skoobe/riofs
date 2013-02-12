import os
import sys
import threading
import time

if len(sys.argv) < 3:
    sys.exit("Usage: {} [num threads] [path]".format(sys.argv[0]))

class ThreadClass(threading.Thread):
    def run(self):
        total_files = 0
        total_dirs = 0
        for dirname, dirnames, filenames in os.walk(sys.argv[2]):
            for subdirname in dirnames:
                total_dirs += 1
            for filename in filenames:
                total_files += 1
        print "dirs: {}, files: {}".format(total_dirs, total_files)

class Main():
    def runit(self):
        t_list = []

        for i in range(int(sys.argv[1])):
            t = ThreadClass()
            time.sleep(0.1)
            t.start()
            t_list.append(t);

        for item in t_list:
            t.join()


m = Main()
start = time.time()
m.runit()
print time.time() - start, "seconds"
