#!/usr/bin/env sh

queue=${1-./jobs.params}
worker=${2-./job.mk}

while read params; do
  make -f $worker -q $params || exit $?;
done < $queue
exit 0;
