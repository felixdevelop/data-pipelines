
class PipelineBase(object):
    source = None
    target = None

    def __init__(self, source, target):
        self.source = source
        self.target = target

    def send(self, *args, **kwargs):
        raise NotImplementedError


class Pipeline(PipelineBase):
    name = None

    def __init__(self, source, target, name=None, **kwargs):
        self.name = name
        super(Pipeline, self).__init__(source, target)

    def send(self, carrier):
        carrier.position += 1
        self.target.dock(carrier)
