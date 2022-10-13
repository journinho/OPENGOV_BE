#!/bin/bash

find ./py_folder -name *.py -print0 | while read -d $'\0' py_file
do
    python "$py_file"
done