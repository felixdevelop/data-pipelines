from copy import deepcopy
from functools import lru_cache
from collections import Iterable
from itertools import groupby


def flatten(items):
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            yield from flatten(x)
        else:
            yield x


def unique(sequence):
    seen = set()
    return [x for x in sequence if not (x in seen or seen.add(x))]


class Path:
    __SEPARATOR = "/"
    __str_path = ""
    __list_path = []

    def __init__(self, path=None, separator=__SEPARATOR):
        if not path:
            path = ""

        if isinstance(path, str):
            self.__str_path = path.replace(separator, self.__SEPARATOR)
            self.__list_path = list(filter(None, path.split(separator)))
        elif isinstance(path, Path):
            self.__str_path = path.__str_path
            self.__list_path = path.__list_path
        elif isinstance(path, Iterable):
            self.__str_path = self.__SEPARATOR.join(path)
            self.__list_path = list(filter(None, path))
        else:
            raise TypeError("path should be 'str' or Path or any Iterable")

    def __hash__(self):
        return hash(self.__str_path)

    def __len__(self):
        return len(self.__list_path)

    def __iter__(self):
        return iter(self.__list_path)

    def __str__(self):
        return " -> ".join(self.__list_path)

    def __repr__(self):
        return self.__str_path

    def __eq__(self, other):
        return self.__str_path == other.__str_path

    def __ne__(self, other):
        return self.__str_path != other.__str_path

    def __gt__(self, other):
        return len(self.__list_path) > len(other.__list_path)

    def __lt__(self, other):
        return len(self.__list_path) < len(other.__list_path)

    def __ge__(self, other):
        return len(self.__list_path) >= len(other.__list_path)

    def __le__(self, other):
        return len(self.__list_path) <= len(other.__list_path)

    def __bool__(self):
        return not self.is_empty()

    def __getitem__(self, x):
        return self.__list_path[x]

    def is_empty(self):
        return len(self.__list_path) == 0

    def index(self, *args, **kwargs):
        return self.__list_path.index(*args, **kwargs)


class LogicalPath(Path):

    __nodes_list = []

    __physical_path = Path()
    __gates = Path()

    @lru_cache(maxsize=1024)
    def get_data_trace(self):

        nodes_list = self.__nodes_list

        nodes_numbers = [[] for _ in range(len(nodes_list))]
        trace = [[] for _ in range(len(nodes_list))]

        trace[0].append(None)

        for i, node in enumerate(nodes_list[1:], start=1):
            for j, node_element in enumerate(node):

                if not isinstance(node_element, (list, tuple, set)):
                    node_element = [node_element]

                nodes_numbers[i] += [j] * len(node_element)

        for i, node_numbers in enumerate(nodes_numbers[1:], start=1):

            flatten_path_node = [k for k, g in groupby(flatten(nodes_list[i - 1]))]

            for j in node_numbers:
                if flatten_path_node[j] is None:
                    trace[i].append(trace[i - 1][j])
                else:
                    trace[i].append(flatten_path_node[j])

        return trace

    @property
    @lru_cache(maxsize=1024)
    def inputs(self):

        trace = self.get_data_trace()

        inputs = {}

        for i, trace_node in enumerate(trace):
            flatten_path_node = flatten(self.__nodes_list[i])

            for j, flatten_path_node_element in enumerate(flatten_path_node):
                if flatten_path_node_element is not None:

                    node_inputs_list = inputs.get(flatten_path_node_element, [])

                    if trace_node[j] != flatten_path_node_element and trace_node[j]:
                        node_inputs_list.append(trace_node[j])
                        inputs[flatten_path_node_element] = node_inputs_list

        return inputs

    @property
    @lru_cache(maxsize=1024)
    def outputs(self):

        trace = self.get_data_trace()

        outputs = {}

        for i, trace_node in enumerate(trace):
            flatten_path_node = flatten(self.__nodes_list[i])

            for j, flatten_path_node_element in enumerate(flatten_path_node):
                if flatten_path_node_element is not None:

                    node_outputs_list = outputs.get(trace_node[j], [])

                    if trace_node[j] != flatten_path_node_element and trace_node[j]:
                        node_outputs_list.append(flatten_path_node_element)
                        outputs[trace_node[j]] = node_outputs_list

        return outputs

    def __init__(self, path, default_gate="main"):
        if not isinstance(path, Iterable):
            raise TypeError("path should be '(list, tuple, set)' or LogicalPath or Path")

        if isinstance(path, Path) or isinstance(path, str):
            super().__init__(path)
            self.__nodes_list = list(self)

        elif isinstance(path, LogicalPath):
            self.__physical_path, self.__gates = path.__physical_path, path.__gates
            self.__nodes_list = deepcopy(path.__nodes_list)
            super().__init__(self.__physical_path)

        elif isinstance(path, (list, set, tuple)):

            flatten_path = list(filter(None, unique(flatten(path))))

            def split_path_node(path_node):
                tokens = path_node.split("#")
                return tokens[0], ''.join(tokens[1:]) or default_gate

            physical_path, gates_path = zip(*map(lambda x: split_path_node(x), flatten_path))

            self.__nodes_list = path
            self.__physical_path, self.__gates = Path(list(physical_path)), Path(gates_path)

            super().__init__(flatten_path)

        else:
            raise TypeError("path should be 'str' or '(list, tuple, set)' or Path")

        self.__nodes_list = [[node] if not isinstance(node, (list, tuple, set)) else node for node in self.__nodes_list]

    @property
    def physical_path(self):
        return self.__physical_path

    @property
    def gates_path(self):
        return self.__gates
