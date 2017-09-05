#!/usr/bin/env bash
gcloud compute ssh  --zone=us-central1-c \
--ssh-flag="-D 1080" \
--ssh-flag="-N" \
--ssh-flag="-n" recommend-m-0 &

/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
--proxy-server="socks5://localhost:1080" \
--host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" \
--user-data-dir=/tmp/
