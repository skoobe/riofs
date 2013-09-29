import sys
import os
import time
import signal
import random
import shutil
import struct
import hashlib
import string
import signal
import collections
import platform

class App ():
    def __init__(self, bucket, base_dir = "/tmp/riofs_test", nr_tests = 10):
        self.bucket = bucket
        self.base_dir = base_dir
        self.nr_tests = nr_tests
        self.read_pid = None
        self.write_pid = None
        self.write_log = "./write.log"
        self.read_log = "./read.log"
        self.test_dir = None
        self.src_dir = self.base_dir + "/orig/"
        self.dst_dir = self.base_dir + "/dest/"
        self.write_dir = self.base_dir + "/write/"
        self.write_cache_dir = self.base_dir + "/write_cache/"
        self.read_dir = self.base_dir + "/read/"
        self.read_cache_dir = self.base_dir + "/read_cache/"
        self.nr_retries = 120
        self.l_files = []
        self.interrupted = False
        self.fuse_block = 4096
        self.verbose = "-v"
        random.seed (time.time())
        self.seed = "1092384956781341341234656953214543219"
        self.words = open("lorem.txt", "r").read().replace("\n", '').split()

    def start_riofs (self, mnt_dir, log_file, cache_dir):
        try:
            pid = os.fork ()
        except OSError, e:
            print "Failed to start RioFS: ", e
            return 0
        
        if pid == 0:
            base_path = os.path.join(os.path.dirname(__file__), '..')
            bin_path = os.path.join(base_path, "src")
            cache = "--cache-dir=" + cache_dir
            conf = "--config=../riofs.conf.xml"
            args = [os.path.join(bin_path, "riofs"), self.verbose, "--disable-stats", "-f", conf, "-l", log_file, cache]
            if platform.system() == "Darwin":
                # disable all local caching
                args.extend(["-o", "direct_io"])
            args.extend([self.bucket, mnt_dir])
            print "Starting RioFS: " + " ".join (args)
            try:
                os.execv(args[0], args)
            except OSError, e:
                print "Failed to start RioFS: ", e
                return 0
        else:
            return pid

    def unmount (self, mnt_dir):
        pid = os.fork()
        if pid == 0:
            if platform.system() == "Darwin":
                args = ["umount", mnt_dir, " 2> /dev/null"]
            else:
                args = ["fusermount", "-u", mnt_dir, " 2> /dev/null"]

            os.execv(args[0], args)
        else:
            time.sleep (1)

    # check if both RioFS are still running
    def check_running (self):
        try:
            pid, sts = os.waitpid (self.write_pid, os.WNOHANG)
        except OSError, e:
            print "RioFS instance is not running: ", e
            self.interrupted = True

        try:
            pid, sts = os.waitpid (self.read_pid, os.WNOHANG)
        except OSError, e:
            print "RioFS instance is not running: ", e
            self.interrupted = True
    
    def signal_handler (self, signum, frame):
        self.interrupted = True
        print "Interrupting, please wait .."

    def run (self):
        # install sig handler
        signal.signal (signal.SIGINT, self.signal_handler)

        if os.path.isdir (self.base_dir):
            print "Directory", self.base_dir, "exists !"
            print "Please remove it and try again !"

        try:
            os.mkdir (self.base_dir);
            os.mkdir (self.write_dir);
            os.mkdir (self.read_dir);
            os.mkdir (self.src_dir);
            os.mkdir (self.dst_dir);
            os.mkdir (self.write_cache_dir);
            os.mkdir (self.read_cache_dir);
        except Exception, e:
            print "Failed to create temp dirs !", e
            return
        
        try:
            shutil.rmtree (self.write_log)
        except:
            None
        try:
            shutil.rmtree (self.read_log)
        except:
            None

        print "Launching RioFS instances .."

        self.write_pid = self.start_riofs (self.write_dir, self.write_log, self.write_cache_dir)
        self.read_pid = self.start_riofs (self.read_dir, self.read_log, self.read_cache_dir)

        if self.write_pid == 0 or self.read_pid == 0:
            print "Failed to start RioFS instance !"
            return
        
        print "Waiting for both RioFS instances to initialize .."
        time.sleep (10)
        
        print "Creating list of files .."
        self.create_files ()
        print "Done."

        random.shuffle (self.l_files)

        self.check_running ()

        #print self.l_files

        print "STARTING .."
        failed = False
        i = 1
        total = len (self.l_files)
        
        # XXX: currently test fails with this line enabled !
        #self.test_dir = "test_" + time.strftime("%Y%m%d-%H%M%S") + "/"
        self.test_dir = ""

        for entry in self.l_files:
            self.check_running ()
            if self.interrupted:
                break
            
            time.sleep (1)
            print >> sys.stderr, ">> (" + str (i) + " out of " + str (total) + ") FILE:", entry
            i = i + 1
            res = self.check_file (entry)
            if res == False:
                print "Test failed !"
                failed = True
                break

        if not failed and not self.interrupted:
            print "All tests passed !"

        if not self.interrupted and not failed:
            print "Removing files .."
            
            failed = False
            i = 1
            total = len (self.l_files)

            for entry in self.l_files:
                self.check_running ()
                if self.interrupted:
                    break
                
                time.sleep (1)
                res = self.remove_remote_file_and_check (entry, i, total)
                i = i + 1
                if res == False:
                    print "Test failed !"
                    failed = True
                    break

            if not self.interrupted and not failed:
                print "Files are removed !"

        print "Killing processes .."
        
        try:
            os.kill (self.write_pid, signal.SIGINT)
        except Exception, e:
            print "Failed to kill processes !", e
        
        try:
            os.kill (self.read_pid, signal.SIGINT)
        except Exception, e:
            print "Failed to kill processes !", e

        time.sleep (2)

        try:
            self.unmount (self.write_dir)
        except Exception, e:
            None
        
        print "Removing work directories .."

        try:
            self.unmount (self.read_dir)
        except Exception, e:
            None
        
        try:
            shutil.rmtree (self.base_dir)
        except:
            None

    def fdata (self):
        a = collections.deque (self.words)
        b = collections.deque (self.seed)
        while True:
            yield ' '.join (list (a)[0:1024])
            a.rotate (int (b[0]))
            b.rotate (1)

    def create_file (self, fname, flen):
        g = self.fdata ()
        fout = open (fname, 'w')
        while os.path.getsize (fname) < flen:
            fout.write (g.next())
        # fout.write (os.urandom (flen))
        fout.close ()

    def md5_for_file (self, fname, block_size=2**20):
        fout = open (fname, 'r')
        md5 = hashlib.md5()
        while True:
            if self.interrupted:
                return
            data = fout.read(block_size)
            if not data:
                break
            md5.update(data)
        fout.close ()
        return md5.hexdigest()

    def str_gen (self, size=10, chars=string.ascii_uppercase + string.digits):
        return ''.join (random.choice(chars) for x in range(size))

    def create_files (self):
        total_files = 5 * self.nr_tests
        files_created = 0

        # tiny files < 4kb
        for i in range (0, self.nr_tests):
            self.check_running ()
            if self.interrupted:
                return
            fname = self.str_gen ()
            flen = random.randint (1, 1024 * 4)
            self.create_file (self.src_dir + fname, flen)
            self.l_files.append ({"name":self.src_dir + fname, "len": flen, "md5": self.md5_for_file (self.src_dir + fname)})
            files_created = files_created + 1
            print "Created file " + str (files_created) + " out of " + str (total_files) + " len: " + str (flen) + "b"

        # small files < 1mb
        for i in range (0, self.nr_tests):
            self.check_running ()
            if self.interrupted:
                return

            fname = self.str_gen ()
            flen = random.randint (1, 1024 * 1024 * 1)
            self.create_file (self.src_dir + fname, flen)
            self.l_files.append ({"name":self.src_dir + fname, "len": flen, "md5": self.md5_for_file (self.src_dir + fname)})
            files_created = files_created + 1
            print "Created file " + str (files_created) + " out of " + str (total_files) + " len: " + str (flen) + "b"

        # medium files 6mb - 20mb
        for i in range (0, self.nr_tests):
            self.check_running ()
            if self.interrupted:
                return
           
            fname = self.str_gen ()
            flen = random.randint (1024 * 1024 * 6, 1024 * 1024 * 20)
            self.create_file (self.src_dir + fname, flen)
            self.l_files.append ({"name":self.src_dir + fname, "len": flen, "md5": self.md5_for_file (self.src_dir + fname)})
            files_created = files_created + 1
            print "Created file " + str (files_created) + " out of " + str (total_files) + " len: " + str (flen) + "b"
       
        # large files 30mb - 40mb
        for i in range (0, self.nr_tests):
            self.check_running ()
            if self.interrupted:
                return
 
            fname = self.str_gen ()
            flen = random.randint (1024 * 1024 * 30, 1024 * 1024 * 40)
            self.create_file (self.src_dir + fname, flen)
            self.l_files.append ({"name":self.src_dir + fname, "len": flen, "md5": self.md5_for_file (self.src_dir + fname)})
            files_created = files_created + 1
            print "Created file " + str (files_created) + " out of " + str (total_files) + " len: " + str (flen) + "b"
        
        # Fuse blocks
        for i in range (0, self.nr_tests):
            self.check_running ()
            if self.interrupted:
                return
 
            fname = self.str_gen ()
            flen = self.fuse_block * (i + 1)
            self.create_file (self.src_dir + fname, flen)
            self.l_files.append ({"name":self.src_dir + fname, "len": flen, "md5": self.md5_for_file (self.src_dir + fname)})
            files_created = files_created + 1
            print "Created file " + str (files_created) + " out of " + str (total_files) + " len: " + str (flen) + "b"

    def check_file (self, entry):
        
        # create paths
        out_src_dir = self.write_dir + self.test_dir
        out_src_name = out_src_dir + os.path.basename (entry["name"])

        in_dst_dir = self.read_dir + self.test_dir
        in_dst_name = in_dst_dir + os.path.basename (entry["name"])

        out_dst_name = self.dst_dir + os.path.basename (entry["name"])
        
        print >> sys.stderr, ">> Copying to SRV, from:", entry["name"], " to:", out_src_name
        
        # create a new remote directory to store test files
        if len (self.test_dir) > 0:
            try:
                os.mkdir (out_src_dir);
            except Exception, e:
                print "Failed to create output directory !", e
                self.interrupted = True
                return False

        for i in range (0, self.nr_retries):
            self.check_running ()
            if self.interrupted:
                print "Interrupted !"
                return False
            
            # force "read" instance to lookup for a file, it will fail the first time, as the file doesn't exist yet
            os.path.isfile (in_dst_name)

            try:
                time.sleep (2)
                shutil.copy (entry["name"], out_src_name)
                break
            except:
                print "File not found, sleeping .."
        
        print >> sys.stderr, ">> Copying to LOC, from:", in_dst_name, " to:", out_dst_name
        
        # write can take some extra time (due file release does not wait)
        for i in range (0, self.nr_retries):
            self.check_running ()
            if self.interrupted:
                print "Interrupted !"
                return False
            try:
                time.sleep (2)
                with open(in_dst_name) as f: pass
                break;
            except:
                print "File not found, sleeping ..", in_dst_name

        try:
            shutil.copy (in_dst_name, out_dst_name)
        except Exception, e:
            print "Failed to copy: from ", in_dst_name, " to ", out_dst_name, " Error: ", e
            return False
        
        self.check_running ()
        if self.interrupted:
            print "Interrupted !"
            return False
       
        md5 = self.md5_for_file (out_dst_name)

        if md5 == entry["md5"]:
            print "Files match: ", entry["md5"], " == ", md5
            print "======"
            return True
        else:
            print "Files (", entry["name"], ") DOES NOT match: ", entry["md5"], " != ", md5
            print "======"
            return False

    # remove file on "write" RioFS instance and then check it on the "read" RioFS instance
    def remove_remote_file_and_check (self, entry, i, total):
        # create paths
        out_src_dir = self.write_dir + self.test_dir
        out_src_name = out_src_dir + os.path.basename (entry["name"])
        
        in_dst_dir = self.read_dir + self.test_dir
        in_dst_name = in_dst_dir + os.path.basename (entry["name"])
        
        out_dst_name = self.dst_dir + os.path.basename (entry["name"]) + "_tmp"

        print >> sys.stderr, "Removing (" + str (i) + " out of " + str (total) + ") FILE: ", out_src_name
        try:
            os.remove (out_src_name)
        except OSError, e:
            print "Failed to remove file " + out_src_name + " Error: " + e
            return False
        
        self.check_running ()
        if self.interrupted:
            print "Interrupted !"
            return False
        
        try:
            shutil.copy (in_dst_name, out_dst_name)
        except Exception, e:
            None
        else:
            print "File is still accessible: " + in_dst_name
            return False

        return True

if __name__ == "__main__":
    if len (sys.argv) < 2:
        print "Please run as: " + sys.argv[0] + " [bucket] [work dir] [number of files per type]"
        sys.exit (1)

    if len (sys.argv) == 2:
        app = App (bucket = sys.argv[1])
    elif len (sys.argv) == 3:
        app = App (bucket = sys.argv[1], base_dir = sys.argv[2])
    elif len (sys.argv) == 4:
        app = App (bucket = sys.argv[1], base_dir = sys.argv[2], nr_tests = int (sys.argv[3]))
    else:
        print "Please run as: " + sys.argv[0] + " [bucket] [work dir] [number of files per type]"
        sys.exit (1)

    app.run ()
