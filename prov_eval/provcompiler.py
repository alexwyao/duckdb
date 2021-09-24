from CodeGen import *
from ops.scan import SeqScan 
from ops.join import Join
from ops.groupby import *
import json
from random import randrange

# TEMPORARY (FOR TPCH5)
q_tpch5_inputs = """
select 
        l_extendedprice * (1 - l_discount) as revenue
from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
        and o_orderdate >= date '1994-01-01'
        and o_orderdate < date '1995-01-01'
"""

class ProvCompiler(object):

    def __init__(self, connection, fname='/tmp/out.cpp'):
        self.cpp_file = fname
        self.con = connection
        self.n_threads = 4 
        # NOTE: must be the same as include/thread_fncs.cpp
        self.c_buf = 16

        # TODO: Check that evaluation follows DFS order 
        # i.e. hashj_count correctly maps the hash join to node
        self.hashj_count = 0
        self.groupby_count = 0

    def process_qp(self, qid, fname, timing=False, debug=False, multi_t=False):
        with open(fname) as f:
            q_json = json.load(f)
        print(q_json)

        self.qid = qid

        self.cpp = CppFile(self.cpp_file)

        self.multi_t=multi_t

        # Imports
        self.cpp("#include \"thread_pool.hpp\"")
        #self.cpp("#include <vector>")
        if debug:
            self.cpp("#include <iostream>")
        if timing:
            self.cpp("#include <benchmark/benchmark.h>")
        if multi_t:
            #self.cpp("#include \"thread_pool.hpp\"")
            self.cpp("#include \"thread_fncs.hpp\"")

        with self.cpp.block("void f({})".format("thread_pool& pool" if multi_t else "")):
            # init thread pool if needed
            if multi_t:
                #self.cpp("thread_pool pool;")
                self.cpp("int N_THREADS = pool.get_thread_count();")
            # Perform DFS on qp
            last_var = self.traverse(q_json['tree'])

            # print out final result if debug
            if debug:
                with self.cpp.block("for (const auto& e : {})".format(last_var)):
                    self.cpp("std::cout << e << std::endl;")


        if timing:
            with self.cpp.block("static void BM_f(benchmark::State& state)"):
                if multi_t:
                    self.cpp("thread_pool pool({});".format(self.n_threads))
                with self.cpp.block("for (auto _ : state)"):
                    if multi_t:
                        self.cpp("f(pool);")
                    else:
                        self.cpp("f();")

            self.cpp("BENCHMARK(BM_f);")
            self.cpp("BENCHMARK_MAIN();")

        else:
            with self.cpp.block("int main()"):
                f_args = ""
                if multi_t:
                    self.cpp("thread_pool pool({});".format(self.n_threads))
                    f_args += "pool"

                if timing:
                    self.cpp("timer tmr;")
                    self.cpp("tmr.start();")

                self.cpp("f({});".format(f_args))

                if timing:
                    self.cpp("tmr.stop();")
                    self.cpp("std::cout << tmr.ns() << std::endl;")
                self.cpp("return 0;")

        self.cpp.close()

    # Returns variable name of binary output array
    # ASSUMPTION: Each operator returns 1 output array
    # (not true for nested aggregations)
    def traverse(self, node):
        # DFS traversal, processing lchild[->rchild]->node
        if node['name'] in ["HASH_GROUP_BY", "PERFECT_HASH_GROUP_BY"]:
            child = self.traverse(node['children'][0])

            print("Adding group by")

            # TODO: Support nested aggregations
            # There are two possibilities:
            # 1. Returning updated values
            # 2. Returning binary array
            #q_lineage = """SELECT (s.index+r.range_start) AS input, s.value AS output
            #        FROM PERFECT_HASH_GROUP_BY_{0}_{1}_SINK AS s, SEQ_SCAN_{0}_{1}_RANGE AS r
            #        WHERE r.chunk_id=s.chunk_id ORDER BY s.value""".format(self.qid, 
            #                self.groupby_count)
            q_lineage = """SELECT s.index AS input, r.index AS output
                        FROM {2}_{0}_{1}_SINK AS s, {2}_{0}_{1}_OUT as r
                        WHERE r.value=s.value ORDER BY output""".format(self.qid, self.groupby_count,
                                node['name'])
            self.groupby_count += 1
            res_lineage = self.con.execute(q_lineage).fetchdf()
            print(res_lineage)

            if "count_star()" in node['extra_info']:
                print("Found count star operator")
                # If count star, we can directly use "child" boolean var as input
                groupby = GroupByCount()
                #print(res_lineage['output'].iloc[-1] + 1)
                #print(len(res_lineage))
                #print(res_lineage['input'])
                if self.multi_t:
                    front_thresh = list()
                    back_thresh = list()
                    batch_sizes = list()
                    N = len(res_lineage)
                    batch_size = int(1 + ((N - 1) / self.n_threads))

                    keys = [x for x in res_lineage['output']]
                    for tid in range(self.n_threads):
                        kvid = tid * batch_size
                        # if last batch_size is too large, limit it
                        cur_bs = batch_size
                        if (kvid + batch_size - 1 >= N):
                            cur_bs = N - kvid
                        #print(cur_bs)
                        i = 1
                        while (i < cur_bs and keys[kvid + i - 1] == keys[kvid + i]):
                            #print(i)
                            i += 1

                        while (i < cur_bs and 
                                keys[kvid + i] % self.c_buf != 0):
                            i += 1

                        j = cur_bs - 1

                        if (i != cur_bs):
                            j -= 1
                            while (j >= i and keys[kvid + j] == keys[kvid + j + 1]):
                                j -= 1
                        front_thresh.append(i)
                        back_thresh.append(j + 1)
                        batch_sizes.append(cur_bs)

                    print("THE CHILD IS {}".format(child))
                    groupby.produce_multi_thread_1d(self.cpp, keys, 
                            child, [x for x in res_lineage['input']], 
                            N, res_lineage['output'].iloc[-1] + 1, self.n_threads,
                            front_thresh, back_thresh, batch_sizes)
                else:
                    groupby.produce_single_thread_1d(self.cpp, [x for x in res_lineage['output']], 
                            child, [x for x in res_lineage['input']], 
                            len(res_lineage), res_lineage['output'].iloc[-1] + 1)

                return groupby.get_name()
            elif "sum" in node['extra_info']:
                print("Found sum operator")
                # NOTE: below ONLY WORKS FOR TPCH5, hardcoded for now

                # retrieve column without aggregates
                res_tpch5_inputs = self.con.execute(q_tpch5_inputs).fetchdf()
                print(res_tpch5_inputs)
                groupby = GroupBySum()
                groupby.produce_single_thread_1d(self.cpp, [x for x in res_lineage['output']], 
                        child, [x for x in res_lineage['input']], [x for x in res_tpch5_inputs['revenue']], 
                        len(res_lineage), res_lineage['output'].iloc[-1] + 1)

                return groupby.get_name()
            else:
                raise Exception("Other groupby ops not implemented yet")

        elif node['name'] in ["HASH_JOIN", "todo_joins"]:
            join_index = self.hashj_count
            self.hashj_count += 1
            left = self.traverse(node['children'][0])
            right = self.traverse(node['children'][1])

            print("Adding join")
            # Join left indices:
            #q_left = "PRAGMA show_tables;"
            q_left = "SELECT value from HASH_JOIN_{}_{}_LHS;".format(self.qid, join_index)
            # Join right indices:
            q_right = "SELECT s.index as value from HASH_JOIN_{0}_{1}_SINK as s, HASH_JOIN_{0}_{1}_RHS as r where s.value=r.value;".format(self.qid, join_index)

            # Run selections
            res_left = self.con.execute(q_left).fetchdf() 
            res_right = self.con.execute(q_right).fetchdf()
            print("Number of tuples joined: {}".format(len(res_left)))
            print(self.con.execute("SELECT * from HASH_JOIN_{}_{}_RHS;".format(self.qid, join_index)).fetchdf())
            print(self.con.execute("PRAGMA show_tables;").fetchdf())
            #print(res_right)
            # Do we want to unfurl loops?
            join = Join()
            if (self.multi_t):
                join.produce_multi_thread_1d(self.cpp, left, right, [x for x in res_left['value']],
                        [x for x in res_right['value']], len(res_left))
            else:
                join.produce_single_thread_1d(self.cpp, left, right, [x for x in res_left['value']],
                        [x for x in res_right['value']], len(res_left))

            return join.get_name()

        elif node['name'] in ['SEQ_SCAN']:
            print("Adding Scan")

            # Write out "indices" array of size N
            seq = SeqScan()
            #seq.produce(self.cpp, [randrange(0,2) for _ in range(node['cardinality'])])
            seq.produce(self.cpp, [1 for _ in range(node['cardinality'])])

            return seq.get_name() 
        else:
            print("Non-lineage op")
            # Other ops, if not supported keep recursing
            if len(node['children']) > 0:
                return self.traverse(node['children'][0])
