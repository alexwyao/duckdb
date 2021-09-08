import duckdb
from provcompiler import *
#from context import *
#from pipeline import *
#import json

class ProvEvaluator(object):

    def __init__(self, table=None):
        print("Initializing evaluator")
        self.con = duckdb.connect(database=':memory:', read_only=False)
        self.compiler = ProvCompiler(self.con, '/tmp/prov_eval.cpp')

    def run_query(self, q: "string"):
        self.con.execute(q)

    def run_lineage_query(self, q):
        # enable lineage
        self.con.execute("PRAGMA trace_lineage='ON'")
        # trace query plan
        self.con.execute("PRAGMA enable_profiling='json';")

        # TODO: change fname to include q_id
        self.con.execute("PRAGMA profile_output='/tmp/profiled.json';")
        
        # run query
        q_out = self.con.execute(q).fetchdf()
        print(q_out)

        # cleanup
        self.con.execute("PRAGMA disable_profiling;")
        self.con.execute("PRAGMA trace_lineage='OFF';")

    def compile_evaluator(self, q):
        q_id = self.con.execute("SELECT query_id FROM queries_list WHERE query='{}'".format(q)).fetchdf().iloc[0,0]
        if not q_id:
            return
        print("Compiling query {} ...".format(q_id))

        # TODO: change fname to include q_id
        self.compiler.process_qp(q_id, '/tmp/profiled.json')
        
        
        """
        // Previous pipeline aware code, not needed for now
        self.pipelined_plan = self.create_pipelined_plan('/tmp/profiled.json')
        self.ctx = Context()
        self.pipelined_plan.produce(self.ctx)

        self.code = self.compile_to_func("compiled_q")
        

    def create_pipelined_plan(self, fname):
        # Read JSON object
        with open(fname) as f:
            q_json = json.load(f)
        print(q_json)
        return Pipelines(q_json)
        """
