from __future__ import absolute_import
__author__ = 'breddels'
import numpy as np
import logging
from .dataframe import DataFrame, DataFrameLocal


logger = logging.getLogger("vaex.remote")


# TODO: we should not inherit from local
class DataFrameRemote(DataFrameLocal):
    def __init__(self, name, column_names, dtypes, length_original):
        super(DataFrameRemote, self).__init__(name, '', column_names)
        self._dtypes = dtypes
        for column_name in self.get_column_names(virtual=True, strings=True):
            self._save_assign_expression(column_name)
        self._length_original = length_original
        self._length_unfiltered = length_original
        self._index_end = length_original
        self.fraction = 1

    def is_local(self):
        return False

    def copy(self, column_names=None, virtual=True):
        dtypes = {name: self.dtype(name) for name in self.get_column_names(strings=True, virtual=False)}
        df = DataFrameRemote(self.name, self.column_names, dtypes=dtypes, length_original=self._length_original)
        df.executor = self.executor
        state = self.state_get()
        if not virtual:
            state['virtual_columns'] = {}
        df.state_set(state, use_active_range=True)
        return df

    def trim(self, inplace=False):
        df = self if inplace else self.copy()
        # can we get away with not trimming?
        return df

    def _evaluate_implementation(self, *args, **kwargs):
        return self.executor.evaluate(self, args, kwargs)

    def _shape_of(self, expression, filtered=True):
        # sample = self.evaluate(expression, 0, 1, filtered=False, internal=True, parallel=False)
        # TODO: support this properly
        rows = len(self) if filtered else self.length_unfiltered()
        return (rows,)
        # return (rows,) + sample.shape[1:]

    def dtype(self, expression, internal=False):
        if str(expression) in self._dtypes:
            return self._dtypes[str(expression)]
        else:
            return super().dtype(expression)

    # TODO: would be nice to get some info on the remote dataframe
    # def __repr__(self):
    #     name = self.__class__.__module__ + "." + self.__class__.__name__
    #     return "<%s(server=%r, name=%r, column_names=%r, __len__=%r)> instance at 0x%x" % (name, self.server, self.name, self.column_names, len(self), id(self))
