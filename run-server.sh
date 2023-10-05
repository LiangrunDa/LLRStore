#!/bin/bash

# default values
listenaddress="0.0.0.0"
port="5551"
loglevel="TRACE"
log_dir="./data/server.log"
directory="./data/storage"
strategy="FIFO"
cache_size="30"
membership_name="node"
peer_address=""
peer_port=""
membership_addr=""

# parse command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -b)
      membership_addr="$2"
      shift 2
      ;;
    -y)
      peer_address="$2"
      shift 2
      ;;
    -x)
      peer_port="$2"
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
./membership -n "$membership_name" -a "$listenaddress" &
# not passing -x and -y  if peer_address and peer_port are empty
if [[ "$peer_address" == "" ]] || [[ "$peer_port" == "" ]]; then
  exec ./kv_server -l "$log_dir" -a "$listenaddress" -b "$membership_addr" -p "$port" --ll "$loglevel" -d "$directory" -s "$strategy" -c "$cache_size"
else
  exec ./kv_server -l "$log_dir" -a "$listenaddress" -b "$membership_addr" -p "$port" --ll "$loglevel" -d "$directory" -s "$strategy" -c "$cache_size" -y "$peer_address" -x "$peer_port"
fi