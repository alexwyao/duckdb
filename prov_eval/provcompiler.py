from CodeGen import *
from ops.scan import SeqScan 
from ops.join import Join
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
        with self.cpp.block("int main()"):
            # Perform DFS on qp
            self.traverse(q_json['tree'])

        self.cpp.close()

    # Returns variable name of binary output array
    # ASSUMPTION: Each operator returns 1 output array
    # (not true for nested aggregations)
    def traverse(self, node):
        # DFS traversal, processing lchild[->rchild]->node
        if node['name'] in ["HASH_GROUP_BY", "todo_perfect_groupby"]:
            self.traverse(node['children'][0])
            
            print("Adding group by")

        
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
