#!/bin/bash

for s in 500 
do
	python3 run_1join_1agg.py $s
	g++ /tmp/prov_eval.cpp -std=c++17 -isystem benchmark/include \
	-Lbenchmark/build/src -lbenchmark -lpthread -o mybenchmark
	./mybenchmark
done
