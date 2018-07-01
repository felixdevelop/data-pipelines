import json

from ..nodes import ComputingBlock


class LoadJsonBlock(ComputingBlock):

    def __init__(self, name, pipelines=None, raise_exception=True):
        self.__raise_exception = raise_exception

        super(LoadJsonBlock, self).__init__(name, pipelines)

    def execute(self, data, context=None, carrier=None):
        try:
            return json.loads(data)
        except Exception as e:
            if self.__raise_exception:
                raise e
            return None


class DumpJsonBlock(ComputingBlock):

    def __init__(self, name, pipelines=None, raise_exception=True):
        self.__raise_exception = raise_exception

        super(DumpJsonBlock, self).__init__(name, pipelines)

    def execute(self, data, context=None, carrier=None):
        try:
            return json.dumps(data)
        except Exception as e:
            if self.__raise_exception:
                raise e
            return None
