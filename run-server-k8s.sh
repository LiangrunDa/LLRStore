#!/bin/bash

# default values
listenaddress="0.0.0.0"
port="5551"
loglevel="DEBUG"
log_dir="./data/server.log"
directory="./data/storage"
strategy="FIFO"
cache_size="30"
membership_name="node"
self_host=""
membership_addr=""
export RUST_LOG="info,h2=error,tokio_util=error,hyper=error,tower=error,tonic=error,memberlist=error"
replication_factor="3"
durability_param="2"
consistencyparam="2"
# parse command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -r)
      consistencyparam="$2"
      shift 2
      ;;
    -w)
      durability_param="$2"
      shift 2
      ;;
    -f)
      replication_factor="$2"
      shift 2
      ;;
    -b)
      membership_addr="$2"
      shift 2
      ;;
    -x)
      self_host="$2"
      shift 2
      ;;
    -n)
      membership_name="$2"
      shift 2
      ;;
    -a)
      listenaddress="$2"
      shift 2
      ;;
    -p)
      port="$2"
      shift 2
      ;;
    -l)
      log_dir="$2"
      shift 2
      ;;
    -ll)
      loglevel="$2"
      shift 2
      ;;
    -d)
      directory="$2"
      shift 2
      ;;
    -s)
      strategy="$2"
      shift 2
      ;;
    -c)
      cache_size="$2"
      shift 2
      ;;
    *)
      echo "invalid option: $1" >&2
      exit 1
      ;;
  esac
done

# check if loglevel is "ALL"/"FINEST"  and modify it to "TRACE"
if [[ "$loglevel" == "ALL" ]] || [[ "$loglevel" == "FINEST" ]]; then
  loglevel="TRACE"
fi
loglevel="TRACE"

# run membership
./membership -n "$self_host" -a "$listenaddress" &
if [[ "$self_host" == "kv-server-0" ]]; then 
  exec ./kv_server --consistency-param "$consistencyparam" --replication-factor "$replication_factor" --durability-param "$durability_param"   -l "$log_dir" -a "$listenaddress" -b "$membership_addr" -p "$port" --ll "$loglevel" -d "$directory" -s "$strategy" -c "$cache_size"
else
  exec ./kv_server --consistency-param "$consistencyparam" --replication-factor "$replication_factor" --durability-param "$durability_param" -l "$log_dir" -a "$listenaddress" -b "$membership_addr" -p "$port" --ll "$loglevel" -d "$directory" -s "$strategy" -c "$cache_size" -y kv-server-0.kv-server.default.svc.cluster.local -x 5147
fi
