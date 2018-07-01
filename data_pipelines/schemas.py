from .path import Path
from .nodes import NodeBase
from .pipelines import PipelineBase, Pipeline


class SchemaBase(object):
    nodes = {}
    pipelines = {}

    def add(self, node, name=None, **kwargs):
        raise NotImplementedError

    def connect(self, *nodes, direction="forward", pipeline=None, pipeline_name=None, **kwargs):
        raise NotImplementedError

    def get_pipeline_info(self, node1_name, node2_name):
        raise NotImplementedError

    def is_valid_path(self, path):
        raise NotImplementedError


class Schema(SchemaBase):

    last_node_number = 0
    last_pipeline_number = 0

    _pipelines_index = {}

    def __init__(self):
        self.nodes = {}
        self.pipelines = {}

    def _get_node_obj_class(self, node):
        if isinstance(node, NodeBase):
            node_class = node.__class__
        elif isinstance(node, type) and issubclass(node, NodeBase):
            node_class = node
        elif isinstance(node, str):
            node_class = self.nodes.get(node)
            assert node_class is not None, '%s node not found' % node
        else:
            raise ValueError("node arg is not Node instance")

        return node_class

    def _get_pipeline_obj_class(self, pipeline):
        if isinstance(pipeline, PipelineBase):
            pipeline_class = pipeline.__class__
        elif isinstance(pipeline, type) and issubclass(pipeline, PipelineBase):
            pipeline_class = pipeline
        elif isinstance(pipeline, str):
            pipeline_class = self.pipelines.get(pipeline)
            assert pipeline_class is not None, '%s pipeline not found' % pipeline
        else:
            raise ValueError("pipeline arg is not Pipeline instance")

        return pipeline_class

    def add(self, node, name=None, **kwargs):
        node_class = self._get_node_obj_class(node)

        exists_nodes = self.nodes.keys()

        if name:
            assert name not in exists_nodes, "Node with same name already exists"
        else:

            while not name or name in exists_nodes:
                self.last_node_number += 1
                name = "node#%d" % self.last_node_number

        node_info = {
            "name": name,
            "class": node_class,
            "kwargs": kwargs,
        }

        self.nodes.update({
            name: node_info
        })

        return name

    def connect(self, *nodes, direction="forward", pipeline=None, **kwargs):
        if pipeline is None:
            pipeline = Pipeline
        else:
            pipeline = self._get_pipeline_obj_class(pipeline)

        if len(nodes) == 1 and isinstance(nodes[0], (list, set, tuple)) and len(nodes[0]) >= 2:
            nodes = nodes[0]

        assert len(nodes) >= 2, "min nodes = 2"

        assert isinstance(direction, str), "direction should be str"
        assert direction in ["forward", "backward", "both"], "direction can be only forward, backward, both"

        def connect_two_nodes(node1_name, node2_name):

            assert isinstance(node1_name, str), "node1_name should be str"
            assert isinstance(node2_name, str), "node2_name should be str"

            exists_pipelines = self.pipelines.keys()

            pipeline_name = None

            while not pipeline_name or pipeline_name in exists_pipelines:
                self.last_pipeline_number += 1
                pipeline_name = "pipeline#%d" % self.last_pipeline_number

            pipeline_info = {
                "name": pipeline_name,
                "class": pipeline,
                "node1": node1_name,
                "node2": node2_name,
                "direction": direction,
                "kwargs": kwargs
            }

            self.pipelines.update({
                pipeline_name: pipeline_info
            })

            def _add_pipeline_info(_n1, _n2):
                first_slice = self._pipelines_index.get(_n1, {})
                first_slice.update({
                    _n2: pipeline_info
                })
                self._pipelines_index[_n1] = first_slice

            if direction in ("forward", "both"):
                _add_pipeline_info(node1_name, node2_name)

            if direction in ("backward", "both"):
                _add_pipeline_info(node2_name, node1_name)

        prev_node = nodes[0]

        for node_name in nodes[1:]:
            connect_two_nodes(prev_node, node_name)
            prev_node = node_name

        return True

    def get_pipeline_info(self, node1_name, node2_name):
        return self._pipelines_index.get(node1_name, {}).get(node2_name)

    def is_valid_path(self, path):
        assert path, "path is empty"
        assert isinstance(path, Path), "path should be Path instance"

        last_node_name = path[0]

        for node_name in path[1:]:
            if not self._pipelines_index.get(last_node_name, {}).get(node_name, {}).get("name"):
                return False
            last_node_name = node_name

        return True
