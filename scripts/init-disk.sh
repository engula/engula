#!/bin/bash

disk=$1
path=$2

sudo mkfs -t ext4 $disk
sudo mkdir $path
sudo mount $disk $path
sudo chmod 0777 -R $path