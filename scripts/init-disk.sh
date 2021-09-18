#!/bin/bash

device=$1

sudo mkfs -t ext4 $device
sudo mkdir /data
sudo mount $device /data
sudo chmod 0777 -R /data