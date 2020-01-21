import numpy as np
import uuid

import vaex
from vaex.remote import DataFrameRemote


def create_df(name, info, executor):
    _dtypes = {name: np.dtype(dtype) for name, dtype in info['dtypes'].items()}
    df = DataFrameRemote(name=name,
                         length_original=info['length_original'],
                         column_names=info['column_names'],
                         dtypes=_dtypes)
    df.executor = executor
    df.state_set(info['state'])
    return df


class Client:
    def __init__(self):
        self.df_map = {}
        self.executor = None

    def close(self):
        raise NotImplementedError

    def connect(self):
        raise NotImplementedError

    def _send(self, msg, msg_id=None):
        raise NotImplementedError

    def update(self):
        self.df_info = self._list()
        self.df_map = {name: create_df(name, info, self.executor) for name, info in self.df_info.items()}

    def __getitem__(self, name):
        if name not in self.df_map:
            raise KeyError("no such DataFrame '%s' at server, possible names: %s" % (name, " ".join(self.df_map.keys())))
        return self.df_map[name]

    def _list(self):
        msg = {'command': 'list'}
        return self._send(msg)

    def evaluate(self, df, args, kwargs):
        args = [str(k) if isinstance(k, vaex.expression.Expression) else k for k in args]
        msg = {'command': 'evaluate', 'df': df.name, 'state': df.state_get(), 'args': args, 'kwargs': kwargs}
        return self._send(msg)

    def execute(self, df, tasks):
        from vaex.encoding import Encoding
        encoder = Encoding()
        msg_id = str(uuid.uuid4())
        self._msg_id_to_tasks[msg_id] = tuple(tasks)
        task_specs = encoder.encode_list("task", tasks)
        msg = {'command': 'execute', 'df': df.name, 'state': df.state_get(), 'tasks': task_specs}
        try:
            return self._send(msg, msg_id=msg_id)
        finally:
            del self._msg_id_to_tasks[msg_id]
