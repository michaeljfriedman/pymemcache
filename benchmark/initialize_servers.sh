#!/bin/bash

hosts=$1
cmd="$2"

if [ -z "$cmd" ]; then
  cmd="sudo yum -y update && sudo yum -y install memcached && sudo systemctl start memcached"
fi

echo "- [global] Running command: $cmd on all hosts..."

for host in $hosts; do
  echo "- [$host] Running command..."
  ssh -i ~/.ssh/aws_key.pem -o "UserKnownHostsFile /dev/null" -o "StrictHostKeyChecking no" ec2-user@$host "$cmd"
  echo "+ [$host] Finished..."
done

echo "+ [global] Finished all hosts..."
