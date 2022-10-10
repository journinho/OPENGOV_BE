#!/bin/bash

for py_file in $(find ./py_folder -name *.py)
do
    python $py_file
done