
class NodeBase(object):
    name = None
    pipelines_links = {}

    def __init__(self, name):
        assert isinstance(name, str), "name should be str"
        self.name = name

    def add_pipeline(self, pipeline):
        raise NotImplementedError

    def get_pipeline(self, node_name):
        raise NotImplementedError

    def dock(self, carrier):
        raise NotImplementedError

    def send(self, carrier):
        raise NotImplementedError


class Node(NodeBase):

    def __init__(self, name, pipelines=None, *args, **kwargs):
        super(Node, self).__init__(name)

        if pipelines:
            self.add_pipelines(pipelines)

    def add_pipeline(self, pipeline):
        self.pipelines_links[pipeline.target.name] = pipeline

    def add_pipelines(self, pipelines):
        for pipeline in pipelines:
            self.add_pipeline(pipeline)

    def get_pipeline(self, node_name):
        return self.pipelines_links[node_name]

    def process(self, carrier):
        raise NotImplementedError

    def dock(self, carrier):
        if self.name != carrier.path[carrier.position]:
            _i = carrier.path.index(self.name, start=carrier.position+1)

            if _i < 0:
                raise RuntimeError("carrier can't dock to node, because node not in path")

            carrier.position = _i

        self.process(carrier)

        if carrier.is_finish():
            carrier.return_at_home()
        else:
            self.send(carrier)

    def send(self, carrier):
        if not carrier.is_finish():
            pipeline = self.get_pipeline(carrier.path[carrier.position+1])
            pipeline.send(carrier)


class ComputingBlock(Node):

    def execute(self, data, context, carrier=None):
        return data

    def process(self, carrier):
        result = self.execute(carrier.data, carrier.context or {}, carrier)
        carrier.data = result
