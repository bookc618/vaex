import ray

import vaex
from vaex.encoding import Encoding, serialize, deserialize
from . import common


class Executor(common.Executor):
    def __init__(self, chunk_size=1_000_000, init=True):
        super().__init__(chunk_size=chunk_size)
        if init:
            ray.init()

    def put(self, obj):
        return ray.put(obj)

    def get(self, obj):
        return ray.get(obj)

    def remote(self, f):
        remote_f = ray.remote(f)
        def caller(*args, **kwargs):
            return remote_f.remote(*args, **kwargs)
        return caller


executor_main = None

def executor():
    global executor_main
    if executor_main is None:
        executor_main = Executor()
    return executor_main
