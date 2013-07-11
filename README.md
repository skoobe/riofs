# RioFS

[![Build Status](https://secure.travis-ci.org/skoobe/riofs.png)](https://travis-ci.org/skoobe/riofs)

RioFS is a userspace filesystem to mount Amazon S3 buckets

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
./autogen.sh
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

Where options are:

```
-v: Verbose output
-f: Do not daemonize process
-c path:  Path to configuration file
-o "opt[,opt...]": fuse options
-l path: Log file to use
--version: Display application version
--help: Display help
```

Please note, that you can specify default S3 service URL (http://s3.amazonaws.com).

You can send USR1 signal to RioFS to re-read the configuration file:
```
killall -s USR1 riofs
```

Send USR2 signal to tell RioFS to reopen log file (useful for logrotate):
```
killall -s USR2 riofs
```

Configuration file
------------------
    
Configuration file (riofs.conf.xml) is located in $(prefix)/etc directory.


Statistics information
------------------

You can enable statistics HTTP server in the configuration file. 
To access stats page use the following URL:
```
http://host:port/stats?access_key=key
```
replace ```host```, ```port``` and ```key``` with the actual values from the configuration file.


Bug reporting
-------------
    
Please include version of riofs and libraries by running:

```
riofs --version
```
