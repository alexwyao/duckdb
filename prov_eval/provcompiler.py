from CodeGen import *
from ops.scan import SeqScan 
from ops.join import Join
from ops.groupby import GroupBy
import json

class ProvCompiler(object):

    def __init__(self, connection, fname='/tmp/out.cpp'):
        self.cpp_file = fname
        self.con = connection

        # TODO: Check that evaluation follows DFS order 
        # i.e. hashj_count correctly maps the hash join to node
        self.hashj_count = 0
        self.groupby_count = 0

    def process_qp(self, qid, fname):
        with open(fname) as f:
            q_json = json.load(f)
        print(q_json)

        self.qid = qid

        self.cpp = CppFile(self.cpp_file)

        # Imports
        self.cpp("#include <iostream>")

        with self.cpp.block("int main()"):
            # Perform DFS on qp
            last_var = self.traverse(q_json['tree'])

            # print out final result
            with self.cpp.block("for (const auto& e : {})".format(last_var)):
                self.cpp("std::cout << e << std::endl;")

        self.cpp.close()

    # Returns variable name of binary output array
    # ASSUMPTION: Each operator returns 1 output array
    # (not true for nested aggregations)
    def traverse(self, node):
        # DFS traversal, processing lchild[->rchild]->node
        if node['name'] in ["HASH_GROUP_BY", "todo_perfect_groupby"]:
            child = self.traverse(node['children'][0])
            
            print("Adding group by")

            # TODO: Support nested aggregations
            # There are two possibilities:
            # 1. Returning updated values
            # 2. Returning binary array
            q_lineage = """SELECT (s.index+r.range_start) AS input, s.value AS output
                    FROM HASH_GROUP_BY_{0}_{1}_SINK AS s, SEQ_SCAN_{0}_{1}_RANGE AS r
                    WHERE r.chunk_id=s.chunk_id ORDER BY s.value""".format(self.qid, 
                            self.groupby_count)
            self.groupby_count += 1
            res_lineage = self.con.execute(q_lineage).fetchdf()
            print(res_lineage)

            if "count_star()" in node['extra_info']:
                print("Found count star operator")
                # If count star, we can directly use "child" boolean var as input
                groupby = GroupBy()
                groupby.produce_single_thread_1d(self.cpp, [x for x in res_lineage['output']], 
                       child, [x for x in res_lineage['input']], 
                       len(res_lineage), res_lineage['output'].iloc[-1] + 1)

                return groupby.get_name()
            else:
                raise Exception("Other groupby ops not implemented yet")
        
        elif node['name'] in ["HASH_JOIN", "todo_joins"]:
            left = self.traverse(node['children'][0])
            right = self.traverse(node['children'][1])

            print("Adding join")
            
            # Join left indices:
            q_left = "SELECT value from HASH_JOIN_{}_{}_LHS;".format(self.qid, self.hashj_count)
            # Join right indices:
            q_right = "SELECT s.index as value from HASH_JOIN_{0}_{1}_SINK as s, HASH_JOIN_{0}_{1}_RHS as r where s.value=r.value;".format(self.qid, self.hashj_count)
            self.hashj_count += 1

            # Run selections
            res_left = self.con.execute(q_left).fetchdf() 
            res_right = self.con.execute(q_right).fetchdf()
            print(res_left)
            print(res_right)
            # Do we want to unfurl loops?
            join = Join()
            join.produce_single_thread_1d(self.cpp, left, right, [x for x in res_left['value']],
                    [x for x in res_right['value']], len(res_left))

            return join.get_name()

        elif node['name'] in ['SEQ_SCAN']:
            print("Adding Scan")

            # Write out "indices" array of size N
            seq = SeqScan()
            seq.produce(self.cpp, [1 for _ in range(node['cardinality'])])

            return seq.get_name() 
        else:
            print("Non-lineage op")
            # Other ops, if not supported keep recursing
            if len(node['children']) > 0:
                return self.traverse(node['children'][0])
