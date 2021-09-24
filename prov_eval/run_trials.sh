#!/bin/bash

for s in 11000 
do
	python3 run_1join_1agg.py $s
	g++ -std=c++17 -g -I include/ /tmp/prov_eval.cpp -isystem benchmark/include \
	-Lbenchmark/build/src -lbenchmark -lpthread -o mybenchmark
	./mybenchmark --benchmark_format=json > out/multi_$s.json
done
