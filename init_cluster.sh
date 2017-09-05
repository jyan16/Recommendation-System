#!/usr/bin/env bash
gcloud dataproc clusters create recommend \
--initialization-actions \
"gs://dataproc-initialization-actions/jupyter/jupyter.sh,\
gs://dataproc-initialization-actions/kafka/kafka.sh" \
--master-machine-type n1-standard-1 \
--worker-machine-type n1-standard-2 \
--num-workers 2 \
--num-masters 3 \
--master-boot-disk-size 50GB \
--worker-boot-disk-size 50GB

