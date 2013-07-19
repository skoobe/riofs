# RioFS

[![Build Status](https://secure.travis-ci.org/skoobe/riofs.png)](https://travis-ci.org/skoobe/riofs)

RioFS is a userspace filesystem to mount Amazon S3 buckets

Requirements
------------

* glib-2.0 >= 2.22
* fuse >= 2.8.3
* libevent >= 2.0
* libxml-2.0 >= 2.6
* libcrypto >= 0.9

All libraries and versions are compatible with Ubuntu 12.04 LTS.

This is a command line to install all requirements to build this project on Ubuntu:

```
sudo apt-get install build-essential gcc make automake autoconf libtool pkg-config intltool libglib2.0-dev libfuse-dev libxml2-dev libevent-dev libssl-dev
```

Please follow the following steps to install the requirements on Centos (tested on Centos 6.2 32bit):
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

How to build RioFS
------------------

```
git clone https://github.com/skoobe/riofs.git
cd riofs
./autogen.sh
./configure
make
sudo make install
```

Provide configure script with --enable-debug-mode flag if you want to get a debug build.


How to start using RioFS
------------------------

Edit ```riofs.conf.xml``` configuration file, by the default it's placed in ```/usr/local/etc/riofs/riofs.conf.xml```.

Launch RioFS:

```
export AWSACCESSKEYID="your AWS access key"
export AWSSECRETACCESSKEY="your AWS secret access key"
riofs [options] [http://s3.amazonaws.com] [bucketname] [mountpoint]
```

Where possible options are:

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

1. make sure ```/etc/fuse.conf``` contains ```user_allow_other``` option.
2. launch RioFS with  ```-o "allow_other"```  parameter.


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
