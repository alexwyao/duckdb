import sys
import pandas as pd
import duckdb
import numpy as np

num_clients = int(sys.argv[1]) 
alpha = 1

def Zipf(a, low, hi, size=None):
    """
    Generate Zipf-like random variables,
    but in inclusive [min...max] interval
    """
    if low == 0:
        raise ZeroDivisionError("")

    v = np.arange(low, hi+1) # values to sample
    p = 1.0 / np.power(v, a)  # probabilities
    p /= np.sum(p)            # normalized

    return np.random.choice(v, size=size, replace=True, p=p)

con = duckdb.connect(database=':memory:', read_only=False)

con.execute("CREATE TABLE clients_left(id INT, zipf INT);")
con.execute("CREATE TABLE clients_right(id INT, zipf INT);")

for i, j in enumerate(Zipf(alpha, 1, num_clients, size=num_clients)):
    con.execute("INSERT INTO clients_left VALUES({},{});".format(i, j))

for i, j in enumerate(Zipf(alpha, 1, num_clients, size=num_clients)):
    con.execute("INSERT INTO clients_right VALUES({},{});".format(i + num_clients, j))

test_q = "SELECT p1.id FROM clients_left p1 INNER JOIN clients_right p2 ON p1.zipf = p2.zipf;"
con.execute("PRAGMA threads=1;")
con.execute("PRAGMA trace_lineage='ON'")
q_out = con.execute(test_q).fetchdf()
con.execute("PRAGMA trace_lineage='OFF'")
print("QUERY RESULT:")
print(q_out)

tables = con.execute("PRAGMA show_tables").fetchdf()
print(tables)

q_id = con.execute("SELECT query_id FROM queries_list WHERE query='{}'".format(test_q)).fetchdf().iloc[0,0]
print("Query id: {}".format(q_id))

print("LHS Table:")
res = con.execute("SELECT * FROM HASH_JOIN_{}_0_LHS;".format(q_id)).fetchdf()
print(res)


print("RHS Table:")
res = con.execute("SELECT * FROM HASH_JOIN_{}_0_RHS;".format(q_id)).fetchdf()
print(res)

print("Sink Table:")
res = con.execute("SELECT * FROM HASH_JOIN_{}_0_SINK;".format(q_id)).fetchdf()
print(res)
