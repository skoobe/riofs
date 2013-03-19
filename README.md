# RioFS

[![Build Status](https://secure.travis-ci.org/skoobe/riofs.png)](https://travis-ci.org/skoobe/riofs)

RioFS (Remote input/output File System) is a userspace filesystem to mount Amazon S3 buckets

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

How to build RioFS
------------------

```
./configure
make
sudo make install
```

Provide configure script with --enable-debug-mode flag if you want to get a debug build.

How to start using RioFS
------------------------

```
export AWSACCESSKEYID="your AWS access key"
export AWSSECRETACCESSKEY="your AWS secret access key"
riofs [options] [http://s3.amazonaws.com] [bucketname] [mountpoint]
```

Where options could be:

```
-v: Verbose output
-f: Do not daemonize process
-c path:  Path to configuration file
--version: Display application version
--help: Display help
```

Please note, that you can specify default S3 service URL (http://s3.amazonaws.com).

Configuration file
------------------
    
Configuration file (riofs.conf.xml) is located in $(prefix)/etc directory.

Bug reporting
-------------
    
Please include version of riofs and libraries by running:

```
riofs --version
```
