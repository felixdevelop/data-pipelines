
class Path:
    __SEPARATOR = "/"
    __str_path = ""
    __list_path = []

    def __init__(self, path=None, separator=__SEPARATOR):
        if not path:
            path = ""

        if isinstance(path, str):
            self.__str_path = path.replace(separator, self.__SEPARATOR)
            self.__list_path = list(filter(None, path.split(separator)))
        elif isinstance(path, Path):
            self.__str_path = path.__str_path
            self.__list_path = path.__list_path
        elif isinstance(path, (list, tuple, set)):
            self.__str_path = self.__SEPARATOR.join(path)
            self.__list_path = list(filter(None, path))
        else:
            raise TypeError("path should be 'str' or '(list, tuple, set)' or Path")

    def __hash__(self):
        return hash(self.__str_path)

    def __len__(self):
        return len(self.__list_path)

    def __iter__(self):
        return iter(self.__list_path)

    def __str__(self):
        return " -> ".join(self.__list_path)

    def __repr__(self):
        return self.__str_path

    def __eq__(self, other):
        return self.__str_path == other.__str_path

    def __ne__(self, other):
        return self.__str_path != other.__str_path

    def __gt__(self, other):
        return len(self.__list_path) > len(other.__list_path)

    def __lt__(self, other):
        return len(self.__list_path) < len(other.__list_path)

    def __ge__(self, other):
        return len(self.__list_path) >= len(other.__list_path)

    def __le__(self, other):
        return len(self.__list_path) <= len(other.__list_path)

    def __bool__(self):
        return not self.is_empty()

    def __getitem__(self, x):
        return self.__list_path[x]

    def is_empty(self):
        return len(self.__list_path) == 0

    def index(self, *args, **kwargs):
        return self.__list_path.index(*args, **kwargs)
