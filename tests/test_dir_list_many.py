import sys
import threading
import time
import test_dir_list

THREAD_DELAY = 0.1
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
        sys.exit("Usage: {} [num threads] [path]".format(sys.argv[0]))

    t_list = []
    numthreads = int(sys.argv[1])
    path = sys.argv[2]
    runlog = []
    dir_counters = []
    file_counters = []

    # first run
    test_dir_list.listrun(1, path, runlog)

    # threaded runs
    for i in range(2, numthreads + 2):
        time.sleep(THREAD_DELAY)
        t = ThreadDirList(i, path, runlog)
        t.start()
        t_list.append(t)

    for t in t_list:
        t.join()

    # sorting by 'a' (run sequence number)
    runlog.sort()

    # print results for each run
    for run in runlog:
        test_dir_list.printrun(run)

        # test if requests are cached:
        if run['a'] > 1 and run['duration'] > CACHE_HIT_TIME:
            print "FAILED: Follow-up request not cached"
            sys.exit(1)

        dir_counters.append(run['dirs'])
        file_counters.append(run['files'])

    # test if all dir and file counters match
    if len(set(dir_counters)) > 1 or len(set(file_counters)) > 1:
        print "FAILED: Not all directory and file counters are equal"
        sys.exit(1)

    print "PASSED"

if __name__ == "__main__":
    main()
