# junixsocket "pre-1.4" fork


## Purpose

This fork has been made to update junixsocket for https://github.com/grzegorz-dubicki/jedis/tree/2.1.0-with-unix-sockets project.

## Features

* Using pre-1.4 codebase
* Updated to Java 1.6
* Minor fixes to make build process easier

## Building

Tested & working on Ubuntu 12.04 LTS 64-bit with Oracle JDK 1.6.0_45 64-bit, Ant 1.9.3, gcc 4.6.3 and glibc 2.15.

1. Install JDK 1.6 & set JAVA_HOME
2. Download Ant, unpack and set ANT_HOME to its dir & add $ANT_HOME/bin to PATH
3. Build with `ant -Dskip32=true` . This should end with "BUILD SUCCESSFUL".
4. Find built JARs in ./build dir.

## Thanks

Thanks to:
* Dr Christian Kohlschütter, author of junixsocket
* anonymous (?) authors of http://svn2github.com/ tool