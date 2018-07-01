import time

from http.server import BaseHTTPRequestHandler, HTTPServer

from data_pipelines.carriers import Carrier
from data_pipelines.networks import Network
from data_pipelines.nodes import ComputingBlock

from data_pipelines.schemas import Schema
from data_pipelines.supervisor import Supervisor


class Cache(object):
    __cache = {}
    __cache_expires = {}

    def get(self, key):
        value, timestamp = self.__cache.get(key, (None, 0))
        if value is None:
            return value

        expires = self.__cache_expires.get(key)

        if 0 <= expires < time.time() - timestamp:
            self.remove(key)
            return None

        return value

    def set(self, key, value, expires=-1):
        self.__cache[key] = (value, time.time())
        self.__cache_expires[key] = expires

        return value

    def remove(self, key):
        del self.__cache[key]
        del self.__cache_expires[key]


cache = Cache()


class CacheBlock(ComputingBlock):

    def __init__(self, name, pipelines=None, action="set", next_station=None, key_prefix="_", cache_key="",
                 cache_expires=-1, cache_key_attr="cache_key", cache_expires_attr="cache_expires", ):

        self.__action = action
        self.__next_station = next_station
        self.__key_prefix = key_prefix
        self.__cache_key = cache_key
        self.__cache_expires = cache_expires
        self.__cache_key_attr = cache_key_attr
        self.__cache_expires_attr = cache_expires_attr

        super(CacheBlock, self).__init__(name, pipelines)

    def execute(self, data, context, carrier=None):

        cache_key = None
        cache_expires = None

        if isinstance(data, dict):
            cache_key = data.get(self.__cache_key_attr, self.__cache_key)
            cache_expires = data.get(self.__cache_expires_attr, self.__cache_expires)

        cache_key = cache_key or context.get(self.__cache_key_attr, self.__cache_key)
        cache_expires = cache_expires or context.get(self.__cache_expires_attr, self.__cache_expires)

        key = "%s_%s" % (self.__key_prefix, cache_key)

        if self.__action == "get":
            cached_data = cache.get(key)

            if cached_data and carrier and self.__next_station:
                carrier.position = carrier.path.index(self.__next_station)

                return cached_data

        elif self.__action == "set":
            cache.set(key, data, expires=cache_expires)

        elif self.__action == "del":
            cache.remove(key)

        return data


class Counter:
    i = 0
    cached = {}


class GenerateCachePath(ComputingBlock):

    def execute(self, data, context, carrier=None):

        for j in range(0, len(Counter.cached.keys())+10):
            if not Counter.cached.get("_"+str(j)):
                Counter.cached["_"+str(j)] = 1
                print("generate %d" % j)
                return "/?n=%d" % j
        return ""


class ExtractRequestData(ComputingBlock):

    def execute(self, data, context, carrier=None):

        path_parts = data.split("?")
        url = path_parts[0]

        params = path_parts[1] if len(path_parts) > 1 else ""

        dict_params = dict([pair.split("=") for pair in params.split("&") if "=" in pair])

        return {
            "cache_key": str(data),
            "request": {
                "url": url,
                "params": dict_params
            }
        }


class ComputingResponse(ComputingBlock):

    @staticmethod
    def factorial_with_pause(n):
        f = 1

        for j in range(1, n + 1):
            f *= j

        time.sleep(2)

        return f

    def execute(self, data, context, carrier=None):

        if not isinstance(data, dict):
            return data

        params = data.get("request", {}).get("params", {})

        n = Counter.i = int(params.get("n", Counter.i + 1))

        f = self.factorial_with_pause(n)

        return dict(data, **{"f": f, "n": n})


class PrepareResponse(ComputingBlock):

    def prepare(self, data, resp):
        if not isinstance(data, dict):
            return data

        return dict(data, **{
            "resp": resp
        })

    def execute(self, data, context, carrier=None):
        if not isinstance(data, dict):
            return self.prepare(data, "Bad request")

        if data.get("request", {}).get("url") != "/":
            return self.prepare(data, "Bad request")

        f = data.get("f", 0)
        n = data.get("n", 0)

        if f <= 0 or n <= 0:
            return self.prepare(data, "Bad request")

        return self.prepare(data, "<html><body  style='word-wrap: break-word'><h1>" \
                                  "Hello World!</h1><br><h6>factorial(%d) = %d " % (n, f) + \
                            "</h6><a href='/?n=%d'>Back</a> <a href='/?n=%d'>Next</a></body></html>" % ((n-1) if n > 0 else 0, n+1))


schema = Schema()

schema.add(GenerateCachePath, name="generate")
schema.add(ExtractRequestData, name="extract_request_data")

schema.add(CacheBlock, name="read_cache", action="get", key_prefix="resp", next_station="set_cache")
schema.add(ComputingResponse, name="computing_response")
schema.add(PrepareResponse, name="prepare_response")
schema.add(CacheBlock, name="set_cache", action="set", key_prefix="resp")

path = ["extract_request_data", "read_cache", "computing_response", "prepare_response", "set_cache"]
schema.connect("generate", "extract_request_data")
schema.connect(path)

network = Network(schema)


class ServerHandler(BaseHTTPRequestHandler):

    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        self._set_headers()

        c = Carrier("C", self.path, path=path)
        network.send_carrier(c)

        try:
            content = c.data["resp"]
        except KeyError:
            content = "Server error"

        self.wfile.write(content.encode('utf-8'))


def run(server_class=HTTPServer, handler_class=ServerHandler, port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print('Starting httpd...')
    httpd.serve_forever()


if __name__ == "__main__":
    from sys import argv

    visor = Supervisor(network)

    with visor.new_session("sess"):
        for i in range(10):
            c = Carrier("C%d" % i, path=["generate"] + path)
            visor.add(c, in_thread=True)
        visor.start(in_thread=True)

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
