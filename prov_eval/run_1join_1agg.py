from provquery import *
import random
from random import randrange
import numpy as np
import sys

num_regions = 10
num_clients = int(sys.argv[1]) 
alpha = 1

random.seed(10)
np.random.seed(10)

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

p = ProvEvaluator()

p.run_query("CREATE TABLE clients_left(id INT, gnum INT, zipf INT, present INT);")
p.run_query("CREATE TABLE clients_right(id INT, gnum INT, zipf INT, present INT);")

for i, j in enumerate(Zipf(alpha, 1, num_clients, size=num_clients)):
    p.run_query("INSERT INTO clients_left VALUES({},{},{},{});".format(i, 0, j, randrange(0, 2)))

for i, j in enumerate(Zipf(alpha, 1, num_clients, size=num_clients)):
    p.run_query("INSERT INTO clients_right VALUES({},{},{},{});".format(i + num_clients, 1, j, randrange(0, 2)))

test_q = "SELECT p1.zipf, count(*) FROM clients_left p1 INNER JOIN clients_right p2 ON p1.zipf = p2.zipf GROUP BY p1.zipf;"
#test_q = "SELECT p1.zipf FROM clients_left p1 INNER JOIN clients_right p2 ON p1.zipf = p2.zipf;"
p.run_lineage_query(test_q)
p.compile_evaluator(test_q)
