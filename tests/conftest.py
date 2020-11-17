"""Define fixtures for SwarmSpawner tests."""
import pytest
from unittest.mock import patch
from jupyterhub.tests.mocking import MockHub
from jupyterhub.tests.conftest import app, io_loop, event_loop, ssl_tmpdir
from docker import from_env

from dockerspawner import SwarmSpawner

MockHub.hub_ip = "0.0.0.0"

@pytest.fixture
def swarmspawner_app(app):

    app.config.SwarmSpawner.name_prefix = "jupyterhub-test"
    app.config.SwarmSpawner.default_config = {
        "image": "jupyterhub/singleuser:latest",
        "networks": ["bridge"]
    }

    with patch.dict(app.tornado_settings, {
            "allow_named_servers": True,
            "named_server_limit_per_user": 2,
            "spawner_class": SwarmSpawner
            }):
        yield app

@pytest.fixture(autouse=True, scope="session")
def docker():
    client = from_env()
    try:
        yield
    finally:
        for service in client.services.list():
            if service.name.startswith("jupyterhub-test"):
                service.remove()
