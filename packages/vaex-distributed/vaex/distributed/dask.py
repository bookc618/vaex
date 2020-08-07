import dask
from dask import delayed

import vaex
from vaex.encoding import Encoding, serialize, deserialize
from . import common


class Executor(common.Executor):
    def __init__(self, chunk_size=1_000_000):
        super().__init__(chunk_size=chunk_size)

    def put(self, obj):
        return dask.persist(obj)[0]

    def get(self, obj):
        return obj.compute()

    def remote(self, f):
        return delayed(f)


executor_main = None

def executor():
    global executor_main
    if executor_main is None:
        executor_main = Executor()
    return executor_main
