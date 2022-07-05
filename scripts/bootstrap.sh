#!/bin/bash

# Copyright 2022 The Engula Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

###### CONFIG ######

# The working dir of the target cluster envs.
BASE_DIR=$(pwd)/cluster_test

# The directory which contains the target binary file.
BINARY_DIR=$(pwd)/target/debug/

# The number of servers of this clusters.
NUM_SERVERS=5

# The first port of servers in cluster.
BASE_PORT=21805

export RUST_LOG=engula_server=debug

###### CONFIG ######

function build_cluster_env() {
    mkdir -p ${BASE_DIR}/log
    mkdir -p ${BASE_DIR}/config

    ln -s ${BINARY_DIR}/engula ${BASE_DIR}
    for id in $(seq 1 ${NUM_SERVERS}); do
        mkdir -p ${BASE_DIR}/server/${id}
    done
}

function start_init_server() {
    ulimit -c unlimited
    setsid ${BASE_DIR}/engula start \
        --db ${BASE_DIR}/server/1 \
        --addr "127.0.0.1:${BASE_PORT}" \
        --init \
        >${BASE_DIR}/log/1.log 2>&1 &
}

function server_port() {
    local id=$1
    local index=$((${id} - 1))
    echo $((${BASE_PORT} + ${index}))
}

function list_addrs() {
    # Disable '-x' for string concats
    local oldopt=$-
    set +x

    local addrs=""
    for id in $(seq 1 ${NUM_SERVERS}); do
        if [[ $id != "1" ]]; then
            addrs="${addrs},"
        fi
        addrs="${addrs}127.0.0.1:$(server_port ${id})"
    done
    echo $addrs

    set -$oldopt
}

function join_list() {
    local oldopt=$-
    set +x
    for id in $(seq 1 ${NUM_SERVERS}); do
        echo "--join 127.0.0.1:$(server_port ${id}) "
    done

    set -$oldopt
}

function start_join_server() {
    local id=$1
    local servers="$(join_list)"

    ulimit -c unlimited
    setsid ${BASE_DIR}/engula start \
        --db ${BASE_DIR}/server/${id} \
        --addr "127.0.0.1:$(server_port ${id})" \
        ${servers} \
        >${BASE_DIR}/log/${id}.log 2>&1 &
}

function start_cluster() {
    start_init_server
    for id in $(seq 2 ${NUM_SERVERS}); do
        start_join_server $id
    done
}

if [ ! -d "${BASE_DIR}" ]; then
    build_cluster_env
fi

pushd ${BASE_DIR} >/dev/null

while [[ $# != 0 ]]; do
    case $1 in
    start)
        start_cluster
        exit 0
        ;;
    status)
        ps -ef | grep "engula start" | grep -v grep
        exit 0
        ;;
    stop)
        ps -ef | grep "engula start" | grep -v grep | awk '{print $2}' | xargs kill -9
        exit 0
        ;;
    clean)
        rm -rf ${BASE_DIR}
        exit 0
        ;;
    addrs)
        list_addrs
        exit 0
        ;;
    *)
        break
        ;;
    esac
done

echo "unknow command $1"
echo "  start   - start cluster"
echo "  stop    - stop servers"
echo "  status  - show servers status"
echo "  clean   - clean all staled data"
echo "  addrs   - list addrs of servers in this cluster"
exit 1
