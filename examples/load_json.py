from data_pipelines.carriers import Carrier
from data_pipelines.networks import Network
from data_pipelines.schemas import Schema

from data_pipelines.blocks.json import LoadJsonBlock


def load_json(string):
    schema = Schema()
    schema.add(LoadJsonBlock, name="load_json", raise_exception=False)

    c = Carrier("Carrier", string, path=["load_json"])

    n = Network(schema)
    n.send_carrier(c)

    return c.data


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    if not args:
        data = '{"hello": "world"}'
    else:
        data = args[0]

    print(load_json(data))
