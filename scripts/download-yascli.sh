#!/bin/bash

mkdir crates/rsqlbench-yasdb/yascli
(cd crates/rsqlbench-yasdb/yascli ; wget https://linked.yashandb.com/resource/yashandb-client-23.2.1.100-linux-x86_64.tar.gz -O yascli.tar.gz ; tar xzf yascli.tar.gz)
