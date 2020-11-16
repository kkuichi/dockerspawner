"Tests for SwarmSpawner"

import pytest
from jupyterhub.tests.test_api import add_user, api_request
from dockerspawner import SwarmSpawner

@pytest.mark.asyncio
async def test_start_stop(swarmspawner_app):
    app = swarmspawner_app

    username = "somebody"
    add_user(app.db, app, name=username)
    user = app.users[username]

    assert isinstance(user.spawner, SwarmSpawner)
    token = user.new_api_token()

    print(user.spawner.get_service_config())

    # TODO: start server