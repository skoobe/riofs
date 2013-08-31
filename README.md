# RioFS

[![Build Status](https://secure.travis-ci.org/skoobe/riofs.png)](https://travis-ci.org/skoobe/riofs)

RioFS is a userspace filesystem for Amazon S3 buckets that runs on Linux and MacOSX. It supports versioned and non-versioned buckets in all AWS regions. RioFS development started at [Skoobe](https://www.skoobe.de) as a storage backend for legacy daemons which cannot talk natively to S3. It is licensed under GPL.

### Requirements

* glib-2.0 >= 2.22
* fuse >= 2.7.3
* libevent >= 2.0
* libxml-2.0 >= 2.6
* libcrypto >= 0.9

#### Ubuntu 12.04 LTS

```
sudo apt-get install build-essential gcc make automake autoconf libtool pkg-config intltool\
                     libglib2.0-dev libfuse-dev libxml2-dev libevent-dev libssl-dev
```

#### Centos 6.2

```
sudo yum groupinstall "Development Tools"
sudo yum install glib2-devel fuse-devel libevent-devel libxml2-devel openssl-devel
wget https://github.com/downloads/libevent/libevent/libevent-2.0.21-stable.tar.gz
tar -xzf libevent-2.0.21-stable.tar.gz
cd libevent-2.0.21-stable
./configure && make
sudo make install
sudo echo "/usr/local/lib/" > /etc/ld.so.conf.d/riofs.conf
sudo ldconfig
export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
cd ..
```

#### MacOSX 10.8

* Download and install the Xcode Command Line Tools package. The package can be downloaded from https://developer.apple.com/downloads/ (free Apple Developer ID required).

* Download and install the latest version of FUSE for OS X (http://osxfuse.github.io)

* Add `/usr/local/lib/pkgconfig` to the `PKG_CONFIG_PATH` environment variable.

```
export PKG_CONFIG_PATH=${PKG_CONFIG_PATH:+${PKG_CONFIG_PATH}:}/usr/local/lib/pkgconfig
```

* Tools and libraries can be installed using MacPorts (http://www.macports.org) by running the following command:

```
sudo port install automake autoconf libtool pkgconfig glib2 libevent libxml2
```

### Building RioFS

```
git clone https://github.com/skoobe/riofs.git
cd riofs
./autogen.sh
./configure
make
sudo make install
```

* Provide configure script with `--enable-debug-mode` flag if you want to get a debug build.

### Using RioFS

```
export AWSACCESSKEYID="your AWS access key"
export AWSSECRETACCESSKEY="your AWS secret access key"
riofs [options] [http://s3.amazonaws.com] [bucketname] [mountpoint]
```

#### Options

```
-v: Verbose output
-f: Do not daemonize process
-c path:  Path to configuration file
-o "opt[,opt...]": fuse options
-l path: Log file to use
--version: Display application version
--help: Display help
```

* You can send USR1 signal to RioFS to re-read the configuration file:

```
killall -s USR1 riofs
```

* Send USR2 signal to tell RioFS to reopen log file (useful for logrotate):

```
killall -s USR2 riofs
```

* In order to allow other users to access mounted directory:

1. make sure `/etc/fuse.conf` contains `user_allow_other` option.
2. launch RioFS with  `-o "allow_other"`  parameter.

* On OS X it is recommended to run RioFS with the `-o "direct_io"` parameter.

#### Configuration file

The configuration file's default location is `$(prefix)/etc/riofs.conf.xml` or can be specified by "-c" command line parameter.

#### Statistics server

You can enable a statistics HTTP server in the configuration file.  Use the following URL to access statistics page:

```
http://host:port/stats?access_key=key
```

replace ```host```, ```port``` and ```key``` with the values from the configuration file.

Add ```&refresh=1``` at the end of URL to refresh this page every second.


### Known limitations

* Appending data to an existing file is not supported.

* A file system for the S3 API is a [leaky abstraction](http://en.wikipedia.org/wiki/Leaky_abstraction). Don't expect POSIX file system semantics.

### Contributing

When you send us a bug report please include the version of RioFS and libraries by running:

```
riofs --version
```
