from .baseop import *

class Join(Op):

    def __init__(self):
        super(Join, self).__init__()

    def produce_single_thread_1d(self, ctx, left_var, right_var, c_0, c_1, c_len):
        name = super(Join, self).get_name()
        ctx("int {}[{}] = {{ 0 }};".format(name, c_len))
        
        with ctx.block(""):
            ctx("int c0[] = {{ {} }};".format(str(c_0)[1: -1]))
            ctx("int c1[] = {{ {} }};".format(str(c_1)[1: -1]))
            with ctx.block("for (int i = 0; i < {}; ++i)".format(c_len)):
                ctx("{}[i] = {}[c0[i]] & {}[c1[i]];".format(name, left_var, right_var))


