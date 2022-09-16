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
MODE=debug

# The working dir of the target cluster envs.
BASE_DIR=$(pwd)/cluster_test

# The directory which contains the target binary file.
BINARY_DIR=$(pwd)/target/${MODE}/

# The number of servers of this clusters.
NUM_SERVERS=1

# The first port of servers in cluster.
BASE_PORT=21805

export RUST_LOG=info #,engula_server=debug,engula_client=debug
export ENGULA_ENABLE_PROXY_SERVICE=true

###### CONFIG ######

function build_cluster_env() {
    mkdir -p ${BASE_DIR}/log

    ln -s ${BINARY_DIR}/engula ${BASE_DIR}
    for id in $(seq 1 ${NUM_SERVERS}); do
        mkdir -p ${BASE_DIR}/server/${id}
        mkdir -p ${BASE_DIR}/config/${id}
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

function start_server() {
    local id=$1
    ulimit -c unlimited
    ulimit -n 102400
    setsid ${BASE_DIR}/engula start \
        --conf ${BASE_DIR}/config/${id}.toml \
        >>${BASE_DIR}/log/${id}.log 2>&1 &
}

# start [id]
#   start server or entire cluster if no id specified.
function start() {
    local id=$1
    if [[ "x$id" == "x" ]]; then
        for id in $(seq 1 ${NUM_SERVERS}); do
            start_server $id
        done
    else
        start_server $id
    fi
}

function stop_server() {
    local id=$1
    ps -ef |
        grep "engula start" |
        grep "${BASE_DIR}/config/${id}.toml" |
        grep -v grep |
        awk '{print $2}' |
        xargs kill -9 >/dev/null 2>&1
}

# stop [id]
#   stop server or entire cluster if no id specified.
function stop() {
    local id=$1
    if [[ "x$id" == "x" ]]; then
        for id in $(seq 1 ${NUM_SERVERS}); do
            stop_server $id
        done
    else
        stop_server $id
    fi
}

function setup_cluster() {
    export ENGULA_NUM_CONN=1
    ${BASE_DIR}/engula start \
        --db ${BASE_DIR}/server/1 \
        --cpu-nums 2 \
        --addr "127.0.0.1:${BASE_PORT}" \
        --init \
        --dump-config ${BASE_DIR}/config/1.toml

    for id in $(seq 2 ${NUM_SERVERS}); do
        local servers="$(join_list)"

        ${BASE_DIR}/engula start \
            --db ${BASE_DIR}/server/${id} \
            --cpu-nums 2 \
            --addr "127.0.0.1:$(server_port ${id})" \
            ${servers} \
            --dump-config ${BASE_DIR}/config/${id}.toml
    done
}

if [ ! -d "${BASE_DIR}" ]; then
    build_cluster_env
fi

pushd ${BASE_DIR} >/dev/null

while [[ $# != 0 ]]; do
    cmd=$1
    shift

    case $cmd in
    setup)
        setup_cluster
        start
        exit 0
        ;;
    shutdown)
        stop
        rm -rf ${BASE_DIR}
        exit 0
        ;;
    start)
        start $@
        exit 0
        ;;
    stop)
        stop $@
        exit 0
        ;;
    status)
        ps -ef | grep "engula start" | grep -v grep
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
    help)
        break;
        ;;
    *)
        echo "unknow command $cmd"
        break
        ;;
    esac
done

echo "./scripts/bootstrap.sh [cmd [option]]"
echo "  setup       - setup a new cluster"
echo "  shutdown    - stop cluster and clean all staled data"
echo "  clean       - clean all staled data"
echo "  start [id]  - start server or cluster"
echo "  stop  [id]  - stop server or cluster"
echo "  status      - show servers status"
echo "  addrs       - list addrs of servers in this cluster"
exit 1
