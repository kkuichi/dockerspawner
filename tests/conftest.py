"""Define fixtures for SwarmSpawner tests."""

import pytest
from unittest.mock import patch
from time import sleep
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

def _is_swarm(info):
    return info["Swarm"]["LocalNodeState"] == "active"

@pytest.fixture(autouse=True, scope="session")
def docker():
    client = DockerClient("unix:///var/run/docker.sock")

    already_swarm = client.info()["Swarm"]["LocalNodeState"] == "active"
    if not already_swarm:
        client.swarm.init()

    network = client.networks.create("dockerspawner-test-network", driver="overlay", attachable=True)
    network.connect("dockerspawner-test")

    try:
        yield client
    finally:
        for service in client.services.list():
            if service.name.startswith("jupyterhub-client"):
                service.scale(0)
                for _ in range(10):
                    if not service.tasks():
                        break
                    sleep(1)
                service.remove()

        network.disconnect("dockerspawner-test")
        network.remove()

        if not already_swarm:
            client.swarm.leave(True)
