import os
import sys
import shutil
import unittest
import tempfile
import time

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "ms3"))

from ms3.testing import MS3Server
from s3ffs_server import s3ffsServer, wait_until


class AmazonTestCase(unittest.TestCase):

    def setUp(self):
        self.local = tempfile.mkdtemp()
        self.s3ffs = None

        # In case there occurs an exception during setUp(), unittest
        # doesn't call tearDown(), hence we need to make sure we don't
        # leave any server processes running.
        try:
            self.s3ffs = s3ffsServer("s3ffs-us", mountpoint=self.local).start()
        except Exception:
            self.tearDown()
            raise

    def tearDown(self):
        if self.s3ffs:
            self.s3ffs.stop()

        shutil.rmtree(self.local, True)

    def test_mounted(self):
        self.assertTrue(os.path.ismount(self.local))

    def test_single_file(self):
        content = "Hello, world!"
        path = os.path.join(self.local, "file.txt")
        with open(path, "w") as f:
            f.write(content)
        wait_until(os.path.exists, path)
        self.assertTrue(open(path).read(), content)


if __name__ == "__main__":
    unittest.main()
