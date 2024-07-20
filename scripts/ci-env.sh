#!/usr/bin/bash

echo "LD_LIBRARY_PATH=$(pwd)/crates/rsqlbench-yasdb/yascli/lib:\$LD_LIBRARY_PATH" >>$GITHUB_ENV
