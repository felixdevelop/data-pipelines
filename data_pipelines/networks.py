from .path import Path
from .schemas import SchemaBase


class NetworkBase(object):
    schema = None

    def __init__(self, schema):
        assert isinstance(schema, SchemaBase), "schema should be Schema instance"
        self.schema = schema

    def build(self):
        raise NotImplementedError


class Network(NetworkBase):
    carriers = {}
    nodes = {}

    def __init__(self, schema):
        super(Network, self).__init__(schema)
        self.build()

    def build(self):
        for node_info in self.schema.nodes.values():
            node_name = node_info["name"]
            node_class = node_info["class"]

            node = node_class(node_name, **node_info["kwargs"])

            self.nodes[node_name] = node

        for pipeline_info in self.schema.pipelines.values():
            pipeline_name = pipeline_info["name"]
            pipeline_class = pipeline_info["class"]
            pipeline_direction = pipeline_info["direction"]

            node1_name = pipeline_info["node1"]
            node2_name = pipeline_info["node2"]

            node1 = self.nodes[node1_name]
            node2 = self.nodes[node2_name]

            if pipeline_direction in ["forward", "both"]:
                pipeline = pipeline_class(node1, node2, name=pipeline_name, **pipeline_info["kwargs"])
                node1.add_pipeline(pipeline)

            if pipeline_direction in ["backward", "both"]:
                pipeline = pipeline_class(node2, node1, name=pipeline_name, **pipeline_info["kwargs"])
                node2.add_pipeline(pipeline)

    def send_carrier(self, carrier):
        carrier.network = self

        actual_path = Path(carrier.path[carrier.position:])
        assert self.schema.is_valid_path(actual_path), "Invalid carrier's path"

        actual_node = self.nodes[actual_path[0]]
        actual_node.dock(carrier)

        carrier.network = None
