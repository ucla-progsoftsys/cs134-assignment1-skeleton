#!/bin/bash

scriptDir=$(dirname -- "$(readlink -f -- "$BASH_SOURCE")")
rm -rf ${scriptDir}/distributed/mapreduce/mrtmp.*.tmp
rm -rf ${scriptDir}/distributed/mapreduce/134-mrinput.txt