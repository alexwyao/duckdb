
class Pipeline(list):

    _id = 0

    def __init__(self):
        self.id = Pipeline._id
        Pipeline._id += 1

    def produce(self, ctx):
        ctx.add_line("// --- Pipeline %s ---" % self.id)
        self[-1].produce(ctx)
        ctx.add_line("")

class Pipelines(object):

    def __init__(self, q_json):
        self.pipeline = Pipeline()
        self.pipelines = []
        self.q = q_json["tree"]

        self.make_pipelines(q_json["tree"], self.pipeline)
        self.pipelines.append(self.pipeline)

    def produce(self, ctx):
        # stopping point:
        # need to implement prepare
        # needs to set up parent/child properly
        for pipeline in self.pipelines:
            pipeline.prepare(ctx)

        for pipeline in self.pipelines:
            pipeline.produce(ctx)

    def make_pipelines(self, node, curpipeline):
        if node['name'] in ["HASH_GROUP_BY", "todo_perfect_groupby"]:
            print("Adding groupby")
            newpipeline = Pipeline()
            self.make_pipelines(node['children'][0], newpipeline)
            # cap with bottom/top?
            self.pipelines.append(newpipeline)
        elif node['name'] in ["todo_joins"]:
            self.make_pipelines(node['children'][0], curpipeline)
            curpipeline.append(left)
            self.make_pipelines(node['children'][1], curpipeline)
            curpipeline.append(right)
        else:
            if len(node['children']) > 0:
                self.make_pipelines(node['children'][0], curpipeline)


