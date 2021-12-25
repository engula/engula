#!/usr/bin/env bash

# Copyright 2021 The Engula Authors.
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

set -o errexit
set -o nounset
set -o pipefail

function kill_service() {
  rc=$?
  if pgrep engula; then
    pkill engula
  fi
  exit $rc
}

trap kill_service EXIT

function test_service() {
    local H="$1"
    cargo run -p engula -- journal run "$H":10001 --mem &
    sleep 1
    cargo run -p engula -- storage run "$H":10002 --mem &
    sleep 1
    cargo run -p engula -- kernel run "$H":10003 --journal "$H":10001 --storage "$H":10002 --mem &
    sleep 1
    cargo run --example hash_engine -- --kernel "$H":10003
    pkill engula
}

test_service "localhost"
test_service "127.0.0.1"
test_service "[::1]"
