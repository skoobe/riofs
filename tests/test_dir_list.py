import os
import sys
import threading
import time

if len(sys.argv) < 3:
    sys.exit("Usage: {} [num threads] [path]".format(sys.argv[0]))

dirs_list = []
files_list = []

class ThreadClass(threading.Thread):
    def run(self):
        total_files = 0
        total_dirs = 0
        for dirname, dirnames, filenames in os.walk(sys.argv[2]):
            for subdirname in dirnames:
                total_dirs += 1
            for filename in filenames:
                total_files += 1
        dirs_list.append(total_dirs)
        files_list.append(total_files)

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

        # test if directory and file counters are the same for all threads
        if (len(set(dirs_list)) == 1 and len(set(files_list)) == 1):
            print "OK: dirs: {}, files: {}".format(dirs_list[0], files_list[0])
        else:
            print "ERROR: Not all directory and file counters are equal"


m = Main()
start = time.time()
m.runit()
print time.time() - start, "seconds"
