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
docker run --detach \
  -p 9000:9000 \
  -p 9001:9001 \
  -v "$DATADIR":/data \
  -e "MINIO_ROOT_USER=$MINIO_ROOT_USER" \
  -e "MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD" \
  -e "MINIO_REGION_NAME=$MINIO_REGION_NAME" \
  quay.io/minio/minio server /data --console-address ":9001"
