from common import *
import pandas as pd
import datetime

def test_repr_default(df):
    code = df._repr_mimebundle_()['text/plain']
    assert 'x' in code


def test_repr_html(df):
    ds = df
    code = ds._repr_html_()
    assert 'x' in code

# TODO: it seems masked arrays + evaluate doesn't work well
# might have to do something with serializing it
def test_mask(df_local):
    ds = df_local
    code = ds._repr_html_()
    assert "'--'" not in code
    assert "--" in code

    code = ds._repr_mimebundle_()['text/plain']
    assert "'--'" not in code
    assert "--" in code


def test_repr_expression(df):
    df = df
    assert 'Error' not in repr(df.x)


def test_repr_df_long_string():
    long_string = "Hi there" * 100
    df = vaex.from_arrays(s=[long_string] * 100)
    assert long_string not in repr(df)
    assert long_string not in str(df)
    assert long_string not in df._repr_html_()
    assert long_string not in df._as_html_table(0, 10)

    # as objects
    df = vaex.from_arrays(o=[{"something": long_string}] * 100)
    assert long_string not in repr(df)
    assert long_string not in str(df)
    assert long_string not in df._repr_html_()
    assert long_string not in df._as_html_table(0, 10)


def test_repr_from_pandas():
    dd_dict = {
        'boolean': [True, True, False, None, True],
        'text': ['This', 'is', 'some', 'text', 'so...'],
        'numbers_1': [1, 30, -2, 1.5, 0.000],
        'numbers_2': [1, None, -2, 1.5, 0.000],
        'numbers_3': [1, np.nan, -2, 1.5, 0.000],
        'datetime_1': [pd.NaT, datetime.datetime(2019, 1, 1, 1, 1, 1), datetime.datetime(2019, 1, 1, 1, 1, 1), datetime.datetime(2019, 1, 1, 1, 1, 1), datetime.datetime(2019, 1, 1, 1, 1, 1)],
        'datetime_2': [pd.NaT, None, pd.NaT, pd.NaT, pd.NaT],
        'datetime_3': [pd.Timedelta('1M'), pd.Timedelta('1D'), pd.Timedelta('100M'), pd.Timedelta('2D'), pd.Timedelta('1H')],
        'datetime_4': [pd.Timestamp('2001-1-1 2:2:11'), pd.Timestamp('2001-12'), pd.Timestamp('2001-10-1'), pd.Timestamp('2001-03-1 2:2:11'), pd.Timestamp('2001-1-1 2:2:11')],
        'datetime_5': [datetime.date(2010, 1, 1), datetime.date(2010, 1, 1), datetime.date(2010, 1, 1), datetime.date(2010, 1, 1), datetime.date(2010, 1, 1)],
        'datetime_6': [datetime.time(21, 1, 1), datetime.time(21, 1, 1), datetime.time(21, 1, 1), datetime.time(21, 1, 1), datetime.time(21, 1, 1)],
    }

    # Get pandas dataframe
    dd = pd.DataFrame(dd_dict)
    dd['datetime_7'] = pd.to_timedelta(dd['datetime_2'] - dd['datetime_1'])
    ds = vaex.from_pandas(dd, copy_index=False)
    repr_value = repr(ds)
    str_value = str(ds)
    assert 'NaT' in repr_value
    assert 'NaT' in str_value
