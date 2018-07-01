import csv

try:
    from io import StringIO  # for Python 3.x
except ImportError:
    from StringIO import StringIO  # for Python 2.x

from ..nodes import ComputingBlock


class LoadCsvBlock(ComputingBlock):

    def __init__(self, name, *args, pipelines=None, data_type="list", raise_exception=True, **kwargs):
        assert data_type in ["list", "dict"]

        self.__data_type = data_type
        self.__raise_exception = raise_exception
        self.__args = args
        self.__kwargs = kwargs

        super(LoadCsvBlock, self).__init__(name, pipelines)

    def execute(self, data, context=None, carrier=None):
        try:
            str_io = StringIO(data)
            if self.__data_type == "list":
                return list(csv.reader(str_io, *self.__args, **self.__kwargs))
            elif self.__data_type == "dict":
                return list(csv.DictReader(str_io, *self.__args, **self.__kwargs))
        except Exception as e:
            if self.__raise_exception:
                raise e
            return None


class DumpCsvBlock(ComputingBlock):

    def __init__(self, name, *args, pipelines=None, data_type="list", raise_exception=True, **kwargs):
        assert data_type in ["list", "dict"]

        self.__data_type = data_type
        self.__raise_exception = raise_exception
        self.__args = args
        self.__kwargs = kwargs

        super(DumpCsvBlock, self).__init__(name, pipelines)

    def execute(self, data, context=None, carrier=None):
        try:
            str_io = StringIO()
            if self.__data_type == "list":
                writer = list(csv.writer(str_io, *self.__args, **self.__kwargs))
            elif self.__data_type == "dict":
                writer = csv.DictWriter(str_io, *self.__args, **self.__kwargs)
            else:
                return StringIO()

            for item in data:
                writer.writerow(item)

            return str_io.getvalue()
        except Exception as e:
            if self.__raise_exception:
                raise e
            return None
