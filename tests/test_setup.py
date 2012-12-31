import os
import sys
import shutil
import unittest
import tempfile
import time

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "ms3"))

from ms3.testing import MS3Server
from s3ffs_server import s3ffsServer, wait_until


class SetupTestCase(unittest.TestCase):

    def setUp(self):
        self.local = tempfile.mkdtemp()
        self.remote = tempfile.mkdtemp()
        self.bucket = "test"
        os.mkdir(os.path.join(self.remote, self.bucket))
        self.s3ffs = None

        # In case there occurs an exception during setUp(), unittest
        # doesn't call tearDown(), hence we need to make sure we don't
        # leave any server processes running.
        try:
            MS3Server.start(datadir=self.remote)
            self.s3ffs = s3ffsServer(self.bucket, mountpoint=self.local).start()
        except Exception:
            self.tearDown()
            raise

    def tearDown(self):
        if self.s3ffs:
            self.s3ffs.stop()
        MS3Server.stop()

        shutil.rmtree(self.local, True)
        shutil.rmtree(self.remote, True)

    def test_mounted(self):
        self.assertTrue(os.path.ismount(self.local))

    def test_single_file(self):
        path = os.path.join(self.local, "file.txt")
        with open(path, "w") as f:
            f.write("Hello, world!")
        wait_until(os.path.exists, os.path.join(self.remote, "test", "file.txt"), sleep=.01)
        self.assertTrue(os.path.exists(path))

    def test_single_file_with_delay(self):
        path = os.path.join(self.local, "file.txt")
        with open(path, "w") as f:
            f.write("Hello, world!")
        wait_until(os.path.exists, os.path.join(self.remote, "test", "file.txt"), sleep=.01)
        time.sleep(6) # should be greater than dir_cache_max_time
        self.assertTrue(os.path.exists(path))

    def test_single_content(self):
        content = "Hello, world!"
        path = os.path.join(self.local, "file.txt")
        with open(path, "w") as f:
            f.write(content)
        wait_until(os.path.exists, os.path.join(self.remote, "test", "file.txt"), sleep=.01)
        self.assertTrue(os.path.exists(path))
        self.assertEquals(open(path).read(), content)

    def test_multile_files(self):
        for i in range(0, 50):
            filename = "file%d.txt" % i
            path = os.path.join(self.local, filename)
            with open(path, "w") as f:
                f.write("Hello, world! #%d" % i)
            wait_until(os.path.exists, os.path.join(self.remote, "test", filename), sleep=0.01)
            self.assertTrue(os.path.exists(path))

    def test_multile_files_concurrent_write(self):
        num_concurrency = 50
        for i in range(0, num_concurrency):
            filename = "file%d.txt" % i
            path = os.path.join(self.local, filename)
            with open(path, "w") as f:
                f.write("Hello, world! #%d" % i)
        for i in range(0, num_concurrency):
            filename = "file%d.txt" % i
            path = os.path.join(self.local, filename)
            wait_until(os.path.exists, os.path.join(self.remote, "test", filename), sleep=0.01)
            self.assertTrue(os.path.exists(path))


if __name__ == "__main__":
    unittest.main()
