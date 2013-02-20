import os
import time

def dirwalk(path):
    total_dirs = 0
    total_files = 0
    for dirname, dirnames, filenames in os.walk(path):
        for subdirname in dirnames:
            total_dirs += 1
        for filename in filenames:
            total_files += 1
    return total_dirs, total_files

def listrun(i, path, runlog):
    start = time.time()
    dirs, files = dirwalk(path)
    duration = time.time() - start
    runlog.append({
        'a': i,
        'dirs': dirs,
        'files': files,
        'duration': duration
    })

def printrun(run):
    print "  Run", str(run['a']).rjust(2) + ":", \
        "dirs:", str(run['dirs']) + ", " + \
        "files:", str(run['files']) + ", " + \
        "duration:", run['duration']
