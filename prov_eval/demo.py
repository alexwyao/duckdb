from provquery import *

p = ProvEvaluator()
create_q = "CREATE TABLE fires AS SELECT * FROM read_csv_auto('/tmp/fires_with_dropped_cols.csv') LIMIT 100"
#test_q = "SELECT STAT_CAUSE_DESCR, COUNT(*) FROM fires GROUP BY STAT_CAUSE_DESCR"
test_q = "SELECT f1.STAT_CAUSE_DESCR  FROM fires f1 JOIN fires f2 ON f1.STATE = f2.STATE"

p.run_query(create_q)
p.run_lineage_query(test_q)
p.compile_evaluator(test_q)
