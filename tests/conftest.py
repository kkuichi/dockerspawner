"""Define fixtures for SwarmSpawner tests."""

import pytest
from unittest.mock import patch
from jupyterhub.tests.mocking import MockHub
from jupyterhub.tests.conftest import app, io_loop, event_loop, ssl_tmpdir
from docker import DockerClient
from docker.errors import APIError

from dockerspawner import SwarmSpawner

MockHub.hub_ip = "0.0.0.0"

@pytest.fixture
def swarmspawner_app(app):

    app.config.SwarmSpawner.name_prefix = "jupyterhub-client"
    app.config.SwarmSpawner.default_config = {
        "image": "jupyterhub/singleuser:latest",
        "networks": ["dockerspawner-test-network"]
    }

    with patch.dict(app.tornado_settings, {
            "allow_named_servers": True,
            "named_server_limit_per_user": 2,
            "spawner_class": SwarmSpawner
            }):
        yield app

@pytest.fixture(autouse=True, scope="session")
def docker():
    client = DockerClient("unix:///var/run/docker.sock")

    client.swarm.init()
    network = client.networks.create("dockerspawner-test-network", driver="overlay", attachable=True)
    network.connect("dockerspawner-test")

    try:
        yield
    finally:
        for service in client.services.list():
            if service.name.startswith("jupyterhub-client"):
                service.remove()

        network.disconnect("dockerspawner-test")
        network.remove()
        client.swarm.leave(True)
