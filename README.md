# RioFS [![Build Status](https://secure.travis-ci.org/skoobe/riofs.png)](https://travis-ci.org/skoobe/riofs)

RioFS is a userspace filesystem for Amazon S3 buckets for servers that runs on Linux and MacOSX. It supports versioned and non-versioned buckets in all AWS regions. RioFS development started at [Skoobe](https://www.skoobe.de) as a storage backend for legacy daemons which cannot talk natively to S3. It handles buckets with many thousands of keys and highly concurrent access gracefully.

### Dependencies

* glib >= 2.22
* fuse >= 2.7.3
* libevent >= 2.0
* libxml >= 2.6
* libcrypto >= 0.9

Find here installation guides for [Ubuntu](https://github.com/skoobe/riofs/wiki/Ubuntu), [Centos](https://github.com/skoobe/riofs/wiki/Centos) and [MacOSX](https://github.com/skoobe/riofs/wiki/MacOSX)

### Building

```
./autogen.sh
./configure
make
sudo make install
```

### Using

```
export AWS_ACCESS_KEY_ID="your AWS access key"
export AWS_SECRET_ACCESS_KEY="your AWS secret access key"
riofs [options] [bucketname] [mountpoint]
```

#### Options

```
-v: Verbose output.
-f: Do not daemonize process.
-c path: Path to configuration file.
-o "opt[,opt...]": fuse options
-l path: Log file to use.
-p: Use legacy path-style access syntax.
--uid: Set UID of filesystem owner.
--gid: Set GID of filesystem owner.
--fmode: Set mode for files.
--dmode: Set mode for directories.
```

#### Hints

*   In order to allow other users to access a mounted directory:

    - make sure `/etc/fuse.conf` contains `user_allow_other` option
  
    - launch RioFS with  `-o "allow_other"`  parameter

* On OS X it is recommended to run RioFS with the `-o "direct_io"` parameter
 
* Default configuration is located at `$(prefix)/etc/riofs.conf.xml`

* Use `./configure --enable-debug` to create a debug build

* RioFS comes with a statistics server, have a look at riofs.xml.conf for details

* Send a USR1 signal to tell RioFS to reread the configuration file

* Send a USR2 signal to tell RioFS to reopen log file (useful for logrotate)

### Known limitations

* Appending data to an existing file is not supported.

* Folder renaming is not supported.

* A file system for the S3 API is a [leaky abstraction](http://en.wikipedia.org/wiki/Leaky_abstraction). Don't expect POSIX file system semantics.

### Contribute

* Any help is welcome, just open an issue if you find a bug

* We also need better documentation, testing, tutorials and benchmarks
