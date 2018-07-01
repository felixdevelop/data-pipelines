from threading import Thread, Condition

from .carriers import Carrier
from .networks import Network

RUN_STATUS = "run"
STOP_STATUS = "stop"


def base_executor(cb, visor, carriers, sleep_duration=1):
    import time

    while visor.flags.get("status") == RUN_STATUS:
        cb(carriers)
        time.sleep(sleep_duration)


def clear_memory(carrier):
    carrier.data = None


class Supervisor(object):
    _sessions = {}
    _current_session = None

    class Session(object):
        __name = None
        __visor = None
        __carriers = {}
        __prev_session = None

        __flags = None
        __thread = None

        @property
        def name(self):
            return self.__name

        @property
        def carriers(self):
            return self.__carriers

        @property
        def flags(self):
            return dict(self.__flags)

        def __init__(self, name: str, visor):
            self.__name = name
            self.__visor = visor
            self.__carriers = {}
            self.__prev_session = None
            self.__flags = {"status": RUN_STATUS}

        def __enter__(self):
            self.__prev_session = self.__visor._current_session
            self.__visor._current_session = self

            return self

        def __exit__(self, _type, value, traceback):
            self.__visor._current_session = self.__prev_session
            self.__prev_session = None

        def inherit(self, session):
            self.__name = session.__name
            self.__visor = session.__visor
            self.__carriers = session.__carriers

        def add(self, carrier: Carrier,
                predicate: callable = lambda *args, **kwargs: True,
                in_thread: bool = False,
                callback: callable = None):

            if in_thread:
                carrier_thread_event = Condition()
                carrier_thread = Thread(target=self.send_carrier, args=(carrier, carrier_thread_event))
                carrier_thread.start()

                carrier_thread_info = (carrier_thread, carrier_thread_event)
            else:
                carrier_thread_info = None

            self.__carriers[carrier.name] = (carrier, predicate, callback, carrier_thread_info)

        def remove(self, carrier: Carrier):
            if isinstance(carrier, str):
                del self.__carriers[carrier]
            else:
                del self.__carriers[carrier.name]

        def start(self, in_thread: bool = False):
            self.__flags["status"] = RUN_STATUS

            carriers = self.__carriers

            if in_thread is True:
                self.__thread = Thread(target=self.__visor.executor,
                                       args=(self.process, self), kwargs={"carriers": carriers})
                self.__thread.start()
            else:
                self.__visor.executor(self.process, self, carriers=carriers)

        def stop(self):
            self.__flags["status"] = STOP_STATUS
            if self.__thread:
                self.__thread.join()
                self.__thread = None

        def send_carrier(self, carrier: Carrier, thread_event: Condition = None):
            if thread_event:

                while True:
                    thread_event.acquire()
                    thread_event.wait()

                    if carrier.position == 0:
                        self.send_carrier(carrier)

                    thread_event.release()

            return self.__visor.network.send_carrier(carrier)

        def process(self, carriers: dict):
            for carrier_name, collection_item in list(carriers.items()):
                carrier, predicate, callback, thread_info = collection_item

                if predicate(carrier, visor=self.__visor):
                    if callback:
                        callback(carrier)

                    if thread_info:

                        thread, thread_event = thread_info

                        if thread_event.acquire(blocking=False):
                            thread_event.notify()
                            thread_event.release()
                    else:
                        self.send_carrier(carrier)

    def __init__(self, network: Network, executor: callable=base_executor):
        self.network = network
        self.executor = executor

        self._current_session = self.new_session(name="default", inherit=None)

    def new_session(self, name: str ="session", inherit: object = "default"):
        if name in self._sessions:
            raise ValueError("Session already started")

        session = self.Session(name, self)

        if inherit:
            base_session = inherit
            if not isinstance(inherit, self.Session):
                base_session = self._sessions.get(inherit)
                if base_session is None:
                    raise ValueError("Session %s not found" % str(inherit))

            session.inherit(base_session)

        self._sessions[name] = session

        return session

    def add(self, carrier: Carrier,
            predicate: callable = lambda *args, **kwargs: True,
            in_thread: bool = False,
            callback: callable = None):

        self._current_session.add(carrier, predicate, in_thread, callback)

    def remove(self, carrier: Carrier):
        self._current_session.remove(carrier)

    def start(self, in_thread: bool = False):
        self._current_session.start(in_thread)

    def stop(self):
        self._current_session.stop()

    @property
    def flags(self):
        return self._current_session.flags
