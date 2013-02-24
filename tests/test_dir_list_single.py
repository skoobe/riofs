import sys
import test_dir_list

def main():
    if len(sys.argv) < 2:
        sys.exit("Usage: {} [path]".format(sys.argv[0]))
    path = sys.argv[1]
    runlog = []
    test_dir_list.listrun(1, path, runlog)
    test_dir_list.printrun(runlog[0])
    print "PASSED"

if __name__ == "__main__":
    main()
