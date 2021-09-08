from .baseop import *

class SeqScan(Op):

    def __init__(self):
        super(SeqScan, self).__init__()

    def produce(self, ctx, arr):
        ctx("int[] {} = {{ {} }};".format(super(SeqScan, self).get_name(), 
            str(arr)[1:-1]))
