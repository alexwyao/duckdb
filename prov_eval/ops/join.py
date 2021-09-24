from .baseop import *

class Join(Op):

    def __init__(self):
        super(Join, self).__init__()

    def produce_c0_c1(self, ctx, c_0, c_1):
        ctx("static int c0[] = {{ {} }};".format(str(c_0)[1: -1]))
        ctx("static int c1[] = {{ {} }};".format(str(c_1)[1: -1]))
        #ctx("static const std::vector<int> c0 = {{ {} }};".format(str(c_0)[1: -1]))
        #ctx("static const std::vector<int> c1 = {{ {} }};".format(str(c_1)[1: -1]))

    def produce_single_thread_1d(self, ctx, left_var, right_var, c_0, c_1, c_len):
        name = super(Join, self).get_name()
        #ctx("int *{0} = new int[{1}]();".format(name, c_len))
        ctx("int {}[{}] = {{ 0 }};".format(name, c_len))
        #ctx("std::vector<int> {} ({}, 0);".format(name, c_len))

        with ctx.block(""):
            self.produce_c0_c1(ctx, c_0, c_1)
            with ctx.block("for (int i = 0; i < {}; ++i)".format(c_len)):
                ctx("{}[i] = {}[c0[i]] & {}[c1[i]];".format(name, left_var, right_var))

    def produce_multi_thread_1d(self, ctx, left_var, right_var, c_0, c_1, c_len):
        name = super(Join, self).get_name()
        #ctx("int *{0} = new int[{1}]();".format(name, c_len))
        ctx("int {}[{}] = {{ 0 }};".format(name, c_len))
        #ctx("std::vector<int> {} ({}, 0);".format(name, c_len))

        with ctx.block(""):
            self.produce_c0_c1(ctx, c_0, c_1)

            ctx("int batch_size = 1 + (({} - 1) / N_THREADS);".format(c_len))
            with ctx.block("for (int i = 0; i < N_THREADS; ++i)"):
                ctx("pool.push_task([i, batch_size, &{0}, &{1}, &{2}] {{JoinThreadFnc1d(i, batch_size, {3}, {0}, {1}, c0, c1, {2});}});".format(left_var,
                    right_var, name, c_len))
            ctx("pool.wait_for_tasks();")
