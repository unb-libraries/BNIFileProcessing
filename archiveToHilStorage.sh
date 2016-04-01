#!/usr/bin/env bash
set -e

TMP_OUTPUT_PATH="/home/imaging/tmp_output"
CWD=$(pwd)

cd $TMP_OUTPUT_PATH
rsync -avp bni-lib/ jsanford@hilstorage.hil.unb.ca:/mnt/md1200/lts-archive/BNI
cd $CWD
