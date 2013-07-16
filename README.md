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

In order to allow other users to access mounted directory:
1) make sure ```/etc/fuse.conf``` contains ```user_allow_other``` option.
2) launch RioFS with  ```-o "allow_other"````  parameter.

Configuration file
------------------
    
Configuration file (riofs.conf.xml) is located in $(prefix)/etc directory, or specified by "-c" command line parameter.
Please read comments in configuration file to understand the meanings of values.


Statistics information
------------------

You can enable statistics HTTP server in the configuration file. 
Use the following URL to access statistics page:
```
http://host:port/stats?access_key=key
```
replace ```host```, ```port``` and ```key``` with the actual values from the configuration file.
Add ```&refresh=1``` at the end of URL to refresh this page every second.


Known limitations
------------------
* Appending data to an existing file is not supported.


Bug reporting
-------------
    
Please include the version of RioFS and libraries by running:

```
riofs --version
```
