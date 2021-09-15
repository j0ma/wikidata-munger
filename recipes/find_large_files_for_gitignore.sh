#!/usr/bin/env bash

# Prints all files in given folder that are over 100MB.

find ${1:-.} -size +100M | sed 's|^\./||g' 
