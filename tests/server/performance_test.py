import pytest


@pytest.mark.xfail(reason="_compute_agg needs to get rid of len(self)")
def test_delayed(df_local, client):
    df = df_local
    xmin = df.x.min()
    xmax = df.x.max()
    df_remote = client.get('test')
    passes = df_local.executor.passes
    assert df_remote.x.min() == xmin
    assert df_local.executor.passes == passes + 1

    # off, top passes, bottom does not
    passes = df_local.executor.passes
    df_remote.x.min()
    df_remote.x.max()
    assert df_local.executor.passes == passes + 2

    passes = df_local.executor.passes
    vmin = df_remote.x.min(delay=True)
    vmax = df_remote.x.max(delay=True)
    assert df_local.executor.passes == passes + 1
    assert vmin.get() == xmin
    assert vmax.get() == xmax
