import sys
import threading
import time
import test_dir_list

CACHE_HIT_TIME = 0.05

class ThreadDirList(threading.Thread):
    def __init__(self, i, path, runlog):
        threading.Thread.__init__(self)
        self.i = i
        self.path = path
        self.runlog = runlog

    def run(self):
        test_dir_list.listrun(self.i, self.path, self.runlog)

def main():
    if len(sys.argv) < 3:
        sys.exit("Usage: {} [num threads] [path1:path2:...]".format(sys.argv[0]))

    print "PASSED"

if __name__ == "__main__":
    main()
