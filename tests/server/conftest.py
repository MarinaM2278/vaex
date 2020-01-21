import vaex.server.service
import vaex.server.dummy
import pytest


@pytest.fixture
def client(df_local):
    df = df_local
    service = vaex.server.service.Service({'test': df})
    server = vaex.server.dummy.Server(service)
    client = vaex.server.dummy.Client(server)
    return client
