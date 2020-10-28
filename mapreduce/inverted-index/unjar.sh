#/bin/bash

jars="${1}"
workdir="./classes"

mkdir -p ${workdir}
cd ${workdir}

for jar in $jars; do
    jar xf "../${jar}"
done 
