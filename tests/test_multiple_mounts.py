import os
import sys
import shutil
import unittest
import tempfile
import time

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "ms3"))

from ms3.testing import MS3Server
from s3ffs_server import s3ffsServer, wait_until


class MultipleMountsTestCase(unittest.TestCase):

    def setUp(self):
        self.l1 = tempfile.mkdtemp()
        self.l2 = tempfile.mkdtemp()
        self.remote = tempfile.mkdtemp()
        self.bucket = "test"
        os.mkdir(os.path.join(self.remote, self.bucket))
        self.s1 = self.s2 = None

        # In case there occurs an exception during setUp(), unittest
        # doesn't call tearDown(), hence we need to make sure we don't
        # leave any server processes running.
        try:
            MS3Server.start(datadir=self.remote)
            self.s1 = s3ffsServer(self.bucket, mountpoint=self.l1).start()
            self.s2 = s3ffsServer(self.bucket, mountpoint=self.l2).start()
        except Exception:
            self.tearDown()
            raise

    def tearDown(self):
        if self.s1:
            self.s1.stop()
        if self.s2:
            self.s2.stop()
        MS3Server.stop()

        shutil.rmtree(self.l1, True)
        shutil.rmtree(self.l2, True)
        shutil.rmtree(self.remote, True)

    def test_mounted(self):
        self.assertTrue(os.path.ismount(self.l1))
        self.assertTrue(os.path.ismount(self.l2))

    def test_file_sync(self):
        p1 = os.path.join(self.l1, "file.txt")
        p2 = os.path.join(self.l2, "file.txt")
        with open(p1, "w") as f:
            f.write("Hello, world!")
        wait_until(os.path.exists, os.path.join(self.remote, "test", "file.txt"), sleep=0.01)
        self.assertTrue(wait_until(os.path.exists, p1, throw_exception=False))
        os.listdir(self.l2)
        self.assertTrue(wait_until(os.path.exists, p2, throw_exception=False))

    def test_file_sync_without_listdir(self):
        p1 = os.path.join(self.l1, "file.txt")
        p2 = os.path.join(self.l2, "file.txt")
        with open(p1, "w") as f:
            f.write("Hello, world!")
        wait_until(os.path.exists, os.path.join(self.remote, "test", "file.txt"), sleep=0.01)
        self.assertTrue(wait_until(os.path.exists, p1, throw_exception=False))
        self.assertTrue(wait_until(os.path.exists, p2, throw_exception=False))


if __name__ == "__main__":
    unittest.main()
