from ..nodes import Node


class ReadFileBlock(Node):

    def __init__(self, name, pipelines=None, file_name="", file_name_key="file_name",
                 open_key="r", raise_exception=True):
        self.__file_name = file_name
        self.__file_name_key = file_name_key
        self.__open_key = open_key
        self.__raise_exception = raise_exception

        super(ReadFileBlock, self).__init__(name, pipelines)

    def process(self, carrier):

        file_name = carrier.context.get(self.__file_name_key, self.__file_name)

        try:
            with open(file_name, self.__open_key) as f:
                result = f.read()
        except Exception as e:
            if self.__raise_exception:
                raise e
            result = None

        carrier.data = result


class WriteFileBlock(Node):

    def __init__(self, name, pipelines=None, file_name="", file_name_key="file_name",
                 open_key="w", raise_exception=True):
        self.__file_name = file_name
        self.__file_name_key = file_name_key
        self.__open_key = open_key
        self.__raise_exception = raise_exception

        super(WriteFileBlock, self).__init__(name, pipelines)

    def process(self, carrier):

        file_name = carrier.context.get(self.__file_name_key, self.__file_name)

        try:
            with open(file_name, self.__open_key) as f:
                f.write(carrier.data)
        except Exception as e:
            if self.__raise_exception:
                raise e
