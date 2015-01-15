# RADOS Java
These are Java bindings for librados (C) which use JNA.

By using JNA there is no need for building the bindings against any header
you can use them on any system where JNA and librados are present.

# Ant
## Building
The bindings can be build using Ant, simply run:

$ ant jar

That will produce a .jar file

N.B.: You need to make sure jna.jar is present in /usr/share/java

## Tests
Tests are available under src/test/java and can be run with Ant as well:

$ ant test

# Maven
## Building
$ mvn clean install (-DskipTests)

## Tests
$ mvn test

# Unit Tests
The tests require a running Ceph cluster. By default it will read /etc/ceph/ceph.conf
and use "admin" as a cephx id.

All tests will be performed on the pool "data".

DO NOT RUN THESE TESTS ON A PRODUCTION PLATFORM.

You can overrride this by setting these environment variables:
* RADOS_JAVA_ID
* RADOS_JAVA_CONFIG_FILE
* RADOS_JAVA_POOL

N.B.: You need to make sure jna.jar and junit.jar are present in /usr/share/java
