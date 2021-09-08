from .baseop import *

class GroupBy(Op):

    def __init__(self):
        super(GroupBy, self).__init__()

    def produce_rewrite(self, ctx, values_var, values_order, N):
        out_name = "var_rewrite"
        ctx("int {0}[{1}];".format(out_name, N))
        ctx("int back_array[] = {{ {} }};".format(str(values_order)[1:-1]))
        with ctx.block("for (int i = 0; i < {}; ++i)".format(N)):
            ctx("{0}[i] = {1}[back_array[i]];".format(out_name, values_var))
        return "var_rewrite"

    def produce_single_thread_1d(self, ctx, keys, values_var, values_order, N, out_N):
        name = super(GroupBy, self).get_name()
        ctx("int {}[{}] = {{ 0 }};".format(name, out_N))
        
        with ctx.block(""):
            # Step 1: Rewrite values into sorted order
            reorder_var = self.produce_rewrite(ctx, values_var, values_order, N)
            # Step 2: Perform aggregation
            ctx("int keys[] = {{ {} }};".format(str(keys)[1:-1]))
            with ctx.block("for (int i = 0; i < {}; ++i)".format(N)):
                ctx("{}[keys[i]] += {}[i];".format(name, reorder_var))


