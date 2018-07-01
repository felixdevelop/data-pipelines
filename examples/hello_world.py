from data_pipelines.carriers import Carrier
from data_pipelines.networks import Network
from data_pipelines.schemas import Schema
from data_pipelines.nodes import ComputingBlock

from data_pipelines.blocks.json import LoadJsonBlock
from data_pipelines.blocks.sub_network import SubNetworkBlock


class ExtractData(ComputingBlock):
    def __init__(self, name, pipelines=None, key="data"):
        self.__key = key
        super(ExtractData, self).__init__(name, pipelines=pipelines)

    def execute(self, data, context, carrier=None):
        return data[self.__key]


class PrepareHelloWorld(ComputingBlock):
    def execute(self, data, context, carrier=None):
        if isinstance(data, (list, tuple)):
            data = ','.join(data)
        return data.replace(",", " ").replace(" !", "!").upper()


extract_data_schema = Schema()
extract_data_schema.add(LoadJsonBlock, name="load_json")
extract_data_schema.add(ExtractData, name="extract_data", key="data")
extract_data_schema.connect("load_json", "extract_data")


base_schema = Schema()
base_schema.add(SubNetworkBlock, name="extract_data", scheme=extract_data_schema, path="load_json/extract_data")
base_schema.add(PrepareHelloWorld, name="prepare_hello_world")
base_schema.connect("extract_data", "prepare_hello_world")


def hello_world(string):

    c = Carrier("Carrier", string, path="extract_data/prepare_hello_world")

    n = Network(base_schema)
    n.send_carrier(c)

    return c.data


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    if not args:
        _data = '{"data": ["hello","world","!"]}'
    else:
        _data = args[0]

    print(hello_world(_data))
    print(hello_world('{"data": "let\'s,go,!"}'))
