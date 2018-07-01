from ..path import Path
from ..networks import Network
from ..nodes import Node


class SubNetworkBlock(Node):

    network = None

    def __init__(self, name, pipelines=None, scheme=None, path=None):
        self.network = Network(scheme)
        self.path = Path(path)

        super(SubNetworkBlock, self).__init__(name, pipelines)

    def process(self, carrier):
        carrier_copy = carrier.copy()
        carrier_copy.path = self.path
        carrier_copy.return_at_home()

        self.network.send_carrier(carrier_copy)

        carrier.data = carrier_copy.data
