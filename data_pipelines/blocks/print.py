from ..nodes import Node


class PrintBlock(Node):

    def process(self, carrier):
        print(carrier.data)


class PrintCarrierInfoBlock(Node):

    def process(self, carrier):
        print(carrier, carrier.data, carrier.context)
