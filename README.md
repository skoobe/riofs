# s3ffs

[![Build Status](https://secure.travis-ci.org/skoobe/s3ffs.png)](https://travis-ci.org/skoobe/s3ffs)

s3ffs (S3 Fast File System) is a userspace filesystem to mount Amazon S3 buckets

Requirements
------------

* glib-2.0 >= 2.32
* fuse >= 2.8.4
* libevent >= 2.0
* libxml-2.0 >= 2.6
* libcrypto >= 0.9

All libraries and versions are compatible with Ubuntu 12.04 LTS.

This is a command line to install all requirements to build this project on Ubuntu:

```
sudo apt-get install build-essential gcc make automake autoconf libtool pkg-config intltool libglib2.0-dev libfuse-dev libxml2-dev libevent-dev libssl-dev
```

How to build s3ffs
------------------

```
./configure
make
sudo make install
```

Provide configure script with --enable-debug-mode flag if you want to get a debug build.

How to start using s3ffs
------------------------

```
export AWSACCESSKEYID="your AWS access key"
export AWSSECRETACCESSKEY="your AWS secret access key"
s3ffs [http://s3.amazonaws.com] [bucketname] [options] [mountpoint]
```

Where options could be:

```
-v: Verbose output
-f: Do not daemonize process
-c path:  Path to configuration file
```

Please note, that you can specify default S3 service URL (http://s3.amazonaws.com).

Configuration file
------------------
    
Configuration file (s3ffs.conf.xml) is located in $(prefix)/etc directory.

Bug reporting
-------------
    
Please include version of s3ffs and libraries by running:

```
s3ffs --version
```
