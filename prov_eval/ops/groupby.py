from .baseop import *
import numpy as np

class GroupBy(Op):

    def __init__(self):
        super(GroupBy, self).__init__()

    def produce_rewrite(self, ctx, values_var, values_order, N):
        out_name = "var_rewrite"
        # Use commented out code for temporary rewrite array
        #ctx("int {0}[{1}];".format(out_name, N))
        ctx("const static int back_array[] = {{ {} }};".format(str(values_order)[1:-1]))
        #with ctx.block("for (int i = 0; i < {}; ++i)".format(N)):
        #    ctx("{0}[i] = {1}[back_array[i]];".format(out_name, values_var))
        return "var_rewrite"

class GroupByCount(GroupBy):

    def produce_single_thread_1d(self, ctx, keys, values_var, values_order, N, out_N):
        name = super(GroupByCount, self).get_name()
        #ctx("int *{} = new int[{}]();".format(name, out_N))
        ctx("static int {}[{}] = {{ 0 }};".format(name, out_N))

        with ctx.block(""):
            # Step 1: Rewrite values into sorted order
            reorder_var = self.produce_rewrite(ctx, values_var, values_order, N)
            # Step 2: Perform aggregation
            ctx("const static int keys[] = {{ {} }};".format(str(keys)[1:-1]))
            with ctx.block("for (int i = 0; i < {}; ++i)".format(N)):
                #ctx("{}[keys[i]] += {}[i];".format(name, reorder_var))
                ctx("{}[keys[i]] += {}[back_array[i]];".format(name, values_var))

    def produce_multi_thread_1d(self, ctx, keys, values_var, values_order, N, out_N, 
            N_THREADS, front_thresh, back_thresh, batch_sizes):
        name = super(GroupByCount, self).get_name()
        #ctx("int *{} = new int[{}]();".format(name, out_N))
        ctx("static int {}[{}] = {{ 0 }};".format(name, out_N))

        with ctx.block(""):
            # Step 1: Rewrite values into sorted order
            reorder_var = self.produce_rewrite(ctx, values_var, values_order, N)
            # Step 2: Perform aggregation
            ctx("const static int keys[] = {{ {} }};".format(str(keys)[1:-1]))
            
            ctx("std::vector<int> s_keys(2 * N_THREADS * C_BUF, 0);")
            ctx("std::vector<int> s_vals(2 * N_THREADS * C_BUF, 0);")

            for t_num in range(N_THREADS):
                ctx("pool.push_task([&s_keys, &s_vals] {{GroupbyCountThreadFnc1d({0}, {7}, {6}, {1}, keys, {2}, {3}, std::ref(s_keys), std::ref(s_vals), {4}, {5}, back_array);}});".format(t_num, 
                    N, values_var, name, front_thresh[t_num], back_thresh[t_num], batch_sizes[t_num], t_num * batch_sizes[0]))
            ctx("pool.wait_for_tasks();")

            with ctx.block("for (int i = 0; i < N_THREADS; ++i)"):
                with ctx.block("for (int j = 0; j < C_BUF; ++j)"):
                    ctx("int thread_key = keys[i * {}] + j;".format(batch_sizes[0]))
                    ctx("{}[thread_key] += s_vals[2 * i * C_BUF + j];".format(name))
                ctx("int back_index = (i + 1) * {} - 1;".format(batch_sizes[0]))
                ctx("if (back_index >= {0}) {{ back_index = {0} - 1; }};".format(N))
                ctx("int back_key = keys[back_index];")
                ctx("{}[back_key] += s_vals[(2 * i + 1) * C_BUF];".format(name))



class GroupBySum(GroupBy):

    def produce_single_thread_1d(self, ctx, keys, mask_var, values_order, values, N, out_N):
        name = super(GroupBySum, self).get_name()
        ctx("static float {}[{}] = {{ 0 }};".format(name, out_N))

        with ctx.block(""):
            # Step 1: Rewrite values into sorted order
            reorder_var = self.produce_rewrite(ctx, mask_var, values_order, N)
            # Step 2: Write values needed for aggregation
            # NOTE: this is currently build for TPCH-5, we rely on type being float
            #pretty_vals = np.array(values)
            #np.set_printoptions(precision=20)
            #print(str(pretty_vals)[1:-1])
            ctx("const static float values_arr[] = {{ {} }};".format(str(values)[1:-1]))
            # Step 2: Perform aggregation
            ctx("const static int keys[] = {{ {} }};".format(str(keys)[1:-1]))
            with ctx.block("for (int i = 0; i < {}; ++i)".format(N)):
                #ctx("{}[keys[i]] += {}[i];".format(name, reorder_var))
                ctx("{}[keys[i]] += ((float) {}[back_array[i]]) * values_arr[back_array[i]];".format(name, mask_var))


