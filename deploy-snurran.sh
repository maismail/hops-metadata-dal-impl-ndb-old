#!/bin/bash

if [ $# -ne 1 ] ; then
  echo "usage: <prog> version (e.g., 2.4.0)"
  exit 1
fi
mvn clean package
scp target/hop-metadata-dal-impl-ndb-1.0-SNAPSHOT.jar glassfish@snurran.sics.se:/var/www/hops/ndb-dal-$1.jar

