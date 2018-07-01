from copy import copy

from .networks import Network
from .path import Path


class CarrierBase(object):
    _name = ""
    _path = Path()
    _position = 0
    _data = None
    _context = None

    def __init__(self, name, data=None, context=None, path=None, path_position=0):
        self._name = name
        self._data = data
        self._context = context
        self._path = Path(path or Path())
        self._position = path_position

    def __copy__(self):
        new_obj = type(self)(self._name)
        new_obj._path = self._path
        new_obj._position = self._position
        new_obj._data = self._data
        new_obj._context = self._context

        return new_obj

    def __hash__(self):
        return hash(self._name)

    def __str__(self):
        if self._path:
            path_list = list(self._path)
            path_list[self._position] = "[%s]" % path_list[self._position]
            paths = ": %s" % (Path(path_list), )
        else:
            paths = ""
        return "%s%s" % (self._name, paths)

    def __repr__(self):
        return self._name


class Carrier(CarrierBase):

    _network = None

    def __get_name(self):
        return self._name

    def __get_path(self):
        return self._path

    def __set_path(self, path):
        self._path = Path(path)

    def __get_position(self):
        return self._position

    def __set_position(self, position):
        assert len(self.path) > position
        self._position = position

    def __get_data(self):
        return self._data

    def __set_data(self, data):
        self._data = data

    def __get_context(self):
        return copy(self._context)

    def __get_network(self):
        return self._network

    def __set_network(self, network):
        if isinstance(network, Network):
            self._network = network
        else:
            self._network = None

    name = property(__get_name)
    path = property(__get_path, __set_path)
    position = property(__get_position, __set_position)
    data = property(__get_data, __set_data)
    context = property(__get_context)
    network = property(__get_network, __set_network)

    def is_finish(self):
        return self.position == len(self.path) - 1

    def return_at_home(self):
        self.position = 0

    def copy(self):
        return copy(self)
