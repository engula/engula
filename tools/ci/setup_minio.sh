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

export MINIO_REGION_NAME=us-east-2
export MINIO_ROOT_USER=engulatest
export MINIO_ROOT_PASSWORD=engulatest

DATADIR=$(mktemp -d)
CONFIG_UNAME=$(uname)
case "${CONFIG_UNAME}" in
  Linux)
    curl -O https://dl.min.io/server/minio/release/linux-amd64/minio
    ;;
  Darwin)
    curl -O https://dl.min.io/server/minio/release/darwin-amd64/minio
    ;;
esac

chmod +x minio
./minio server "$DATADIR" &
curl \
    --retry 5 \
    --retry-delay 1 \
    --retry-connrefused \
    http://127.0.0.1:9000/minio/health/live
