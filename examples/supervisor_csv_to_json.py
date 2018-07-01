from data_pipelines.path import Path

from data_pipelines.blocks.csv import LoadCsvBlock
from data_pipelines.blocks.files import ReadFileBlock, WriteFileBlock
from data_pipelines.blocks.json import DumpJsonBlock
from data_pipelines.blocks.print import PrintBlock

from data_pipelines.carriers import Carrier
from data_pipelines.networks import Network

from data_pipelines.schemas import Schema
from data_pipelines.supervisor import Supervisor


schema = Schema()
schema.add(ReadFileBlock, name="read_file")
schema.add(LoadCsvBlock, name="read_csv_data", data_type='dict')
schema.add(DumpJsonBlock, name="json_to_str")
schema.add(WriteFileBlock, name="write_file", file_name_key="write_file")
schema.add(PrintBlock, name="print")

path_for_print_changes = ["read_file", "read_csv_data", "json_to_str", "print"]
path_for_file_write = ["read_file", "read_csv_data", "json_to_str", "write_file"]

schema.connect(path_for_print_changes)
schema.connect(path_for_file_write)

network = Network(schema)

if __name__ == "__main__":
    import sys

    args = sys.argv[1:]

    if len(args) > 0:
        f = sys.argv[1]
        args = args[1:]
    else:
        f = "file.csv"

    visor = Supervisor(network)

    for arg in args:
        carrier1 = Carrier("Carrier for '%s'" % arg, context={"file_name": "file.csv", "write_file": "_file.json"},
                           path=path_for_file_write)

        carrier2 = Carrier("Carrier for '%s'" % arg, context={"file_name": "file.csv", "write_file": arg},
                           path=path_for_file_write)

        with visor.new_session(name="session_%s" % arg, inherit=None) as base_s:
            visor.add(carrier1)

            with visor.new_session(name="session1_%s" % arg, inherit=base_s):
                visor.add(carrier2)
                visor.start(in_thread=True)

            # with visor.new_session(name="session2_%s" % arg, inherit=base_s):
            #     visor.add(carrier3)
            #     visor.start(in_thread=True)

    # default session

    class NewDataPredicateContext:
        old_value = ""
        main_carrier_path = Path()

        def __init__(self, path):
            self.main_carrier_path = path

        def predicate(self, _carrier, **kwargs):

            if _carrier.data != self.old_value and _carrier.data is not None:
                self.old_value = _carrier.data

                _carrier.path = Path(self.main_carrier_path)
            else:
                _carrier.path = Path(self.main_carrier_path[:-1])

            return True


    predicate_class = NewDataPredicateContext(path_for_print_changes)
    main_carrier = Carrier("Carrier", context={"file_name": f}, path=path_for_print_changes)

    visor.add(main_carrier, predicate=predicate_class.predicate)
    visor.start(in_thread=False)
