#!/usr/bin/env bash
set -e
cur_reel=221
end_reel=260

while [ $cur_reel -le $end_reel ]; do
  echo "Starting Reel : $cur_reel"
  python processBNIImages.py --source=/home/imaging/Mike_ready_for_input/$cur_reel --target=/mnt/amazons3fs --bni=/home/imaging/tmp_output/bni-bni --lib=/home/imaging/tmp_output/bni-lib --next=000$cur_reel
  cur_reel=$(($cur_reel+1))
done
