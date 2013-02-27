import sys
import os
import time
import signal
import random
import shutil
import struct
import hashlib
import string

class App ():
    def __init__(self):
        self.read_pid = None
        self.write_pid = None
        self.base_dir = "/tmp/s3ffs_test"
        self.src_dir = self.base_dir + "/orig/"
        self.dst_dir = self.base_dir + "/dest/"
        self.write_dir = self.base_dir + "/write/"
        self.write_cache_dir = self.base_dir + "/write_cache/"
        self.read_dir = self.base_dir + "/read/"
        self.read_cache_dir = self.base_dir + "/read_cache/"
        self.nr_tests = 1
        self.l_files = []
        random.seed (time.time())

    def start_s3ffs (self, mnt_dir, log_file, cache_dir):
        pid = os.fork ()
        if pid == 0:
            base_path = os.path.join(os.path.dirname(__file__), '..')
            bin_path = os.path.join(base_path, "src")
            cache = "--cache-dir=" + cache_dir
            conf = "--config=../s3ffs.conf.xml"
            args = [os.path.join(bin_path, "s3ffs"), "-f", conf, cache, "http://s3.amazonaws.com", "skoobe-test-s3ffs", mnt_dir]
            sys.stdout = open (log_file, 'w')
            os.execv(args[0], args)
        else:
            return pid

    def unmount (self, mnt_dir):
        pid = os.fork()
        if pid == 0:
            args = ["fusermount", "-u", mnt_dir]
            os.execv(args[0], args)

    def run (self):
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
        
        self.write_pid = self.start_s3ffs (self.write_dir, "./write.log", self.write_cache_dir)
        self.read_pid = self.start_s3ffs (self.read_dir, "./read.log", self.read_cache_dir)
        
        print "Creating list of files .."
        self.create_files ()
        print "Done."

        random.shuffle (self.l_files)

        #print self.l_files

        print "STARTING .."
        failed = False
        for entry in self.l_files:
            time.sleep (1)
            print >> sys.stderr, ">> FILE:", entry
            res = self.check_file (entry)
            if res == False:
                print "Test failed !"
                failed = True
                break

        if failed == False:
            print "All tests passed !"

        try:
            print "Killing processes .."
            os.kill (self.write_pid, signal.SIGINT)
            os.kill (self.read_pid, signal.SIGINT)
        except Exception, e:
            print "Failed to kill processes !", e

        time.sleep (2)

        try:
            self.unmount (self.write_dir)
            self.unmount (self.read_dir)
        except Exception, e:
            print "Failed to unmount !", e
        
        if failed == False:
            try:
                shutil.rmtree (self.base_dir)
            except:
                None

    def create_file (self, fname, flen):
        fout = open (fname, 'w')
        fout.write (os.urandom (flen))
        fout.close ()

    def md5_for_file (self, fname, block_size=2**20):
        fout = open (fname, 'r')
        md5 = hashlib.md5()
        while True:
            data = fout.read(block_size)
            if not data:
                break
            md5.update(data)
        fout.close ()
        return md5.hexdigest()

    def str_gen (self, size=10, chars=string.ascii_uppercase + string.digits):
        return ''.join (random.choice(chars) for x in range(size))

    def create_files (self):

        # tiny files < 4kb
        for i in range (0, self.nr_tests):
            fname = self.str_gen ()
            flen = random.randint (1, 1024 * 4)
            self.create_file (self.src_dir + fname, flen)
            self.l_files.append ({"name":self.src_dir + fname, "len": flen, "md5": self.md5_for_file (self.src_dir + fname)})

        # small files < 1mb
        for i in range (0, self.nr_tests):
            fname = self.str_gen ()
            flen = random.randint (1, 1024 * 1024 * 1)
            self.create_file (self.src_dir + fname, flen)
            self.l_files.append ({"name":self.src_dir + fname, "len": flen, "md5": self.md5_for_file (self.src_dir + fname)})

        # medium files 6mb - 20mb
        for i in range (0, self.nr_tests):
            fname = self.str_gen ()
            flen = random.randint (1024 * 1024 * 6, 1024 * 1024 * 20)
            self.create_file (self.src_dir + fname, flen)
            self.l_files.append ({"name":self.src_dir + fname, "len": flen, "md5": self.md5_for_file (self.src_dir + fname)})
        
        # large files 30mb - 40mb
        #for i in range (0, self.nr_tests):
        for i in range (0, 0):
            fname = self.str_gen ()
            flen = random.randint (1024 * 1024 * 30, 1024 * 1024 * 40)
            self.create_file (self.src_dir + fname, flen)
            self.l_files.append ({"name":self.src_dir + fname, "len": flen, "md5": self.md5_for_file (self.src_dir + fname)})
    

    def check_file (self, entry):
        out_src_name = self.write_dir + os.path.basename (entry["name"])
        print >> sys.stderr, ">> Copying to SRV", entry["name"], " to: ", out_src_name

        for i in range (0, 10):
            try:
                time.sleep (2)
                shutil.copy (entry["name"], out_src_name)
                break
            except:
                print "File not found, sleeping .."
        
        in_dst_name = self.read_dir + os.path.basename (entry["name"])

        # TEST
        # shutil.copy (out_src_name, in_dst_name)

        out_dst_name = self.dst_dir + os.path.basename (entry["name"])
        
        print >> sys.stderr, ">> Copying to LOC", in_dst_name, " to: ", out_dst_name
        # write can take some extra time (due file release does not wait)
        for i in range (0, 10):
            try:
                time.sleep (2)
                with open(in_dst_name) as f: pass
                break;
            except:
                print "File not found, sleeping ..", in_dst_name

        try:
            shutil.copy (in_dst_name, out_dst_name)
        except:
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


if __name__ == "__main__":
    app = App ()
    app.run ()
