#!/usr/bin/env sh

limit=${1-20}
queue=${2-./jobs.params}
worker=${3-./job.mk}

jobcount=$(wc -l < $queue)
end=$(($(date +"%s") + $limit));

while read params; do
  make -f $worker -q $params; 
  more=$?;
  if [ $more -ne 0 ]; then
    remains=$(($end-$(date +"%s")));
    total=$(($remains * $jobcount));
    if [ $total -ge $limit ]; then
      make -f $worker $params;
    else
      exit 0;
    fi
  fi
done < $queue
