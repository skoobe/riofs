import os
import time
import signal
import urllib


class WaitException(Exception):
    pass


def wait_until(func, args, timeout=10, sleep=1, throw_exception=True):
    if not isinstance(args, list):
        args = [args]
    t1 = time.time()
    while not func(*args):
        time.sleep(sleep)
        if time.time() - t1 > timeout: # seconds
            if throw_exception:
                raise WaitException("wait_until %s timeout raised", func)
            else:
                return False
    return True


def pid_exists(pid):
    """Check whether pid exists in the current process table."""
    if pid < 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError, e:
        return e.errno == errno.EPERM
    else:
        return True


class s3ffsServer(object):

    def __init__(self, bucket, mountpoint, endpoint="http://localhost:9010"):
        self.bucket = bucket
        self.endpoint = endpoint
        self.mountpoint = mountpoint
        self.pid = None

    def start(self):
        """
            Start the s3ffs server with the provided data directory. This method
            will fork the process and start a server in the child process.
        """
        assert self.pid is None
        self.pid = os.fork()
        if self.pid == 0:
            base_path = os.path.join(os.path.dirname(__file__), '..')
            bin_path = os.path.join(base_path, "src")
            args = [os.path.join(bin_path, "s3ffs"), self.endpoint, self.bucket, "-f", self.mountpoint, "--path-style", "--part-size=2147483647"]
            env = {"AWSACCESSKEYID": os.getenv("AWSACCESSKEYID") or "X",
                   "AWSSECRETACCESSKEY": os.getenv("AWSSECRETACCESSKEY") or "X"}
            os.execve(args[0], args, env)
        else:
            wait_until(os.path.ismount, self.mountpoint)
        return self

    def stop(self):
        """ Stop a started s3ffs Server """
        assert self.pid is not None
        os.kill(self.pid, signal.SIGINT) # try to stop gracefully...
        try:
            wait_until(lambda x:not pid_exists(x), self.pid, sleep=0.01, timeout=1)
        except WaitException:
            os.kill(self.pid, signal.SIGKILL) # ...or just kill it

        wait_until(lambda x:not os.path.ismount(x), self.mountpoint)
