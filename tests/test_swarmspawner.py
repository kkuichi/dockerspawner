"Tests for SwarmSpawner"

import pytest
from jupyterhub.tests.test_api import add_user, api_request
from dockerspawner import SwarmSpawner

@pytest.mark.asyncio
async def test_start_stop(swarmspawner_app):
    username = "has@"
    server_name = "also-has@"

    app = swarmspawner_app
    add_user(app.db, app, name=username)
    user = app.users[username]

    spawner = user.spawners[server_name]
    assert isinstance(spawner, SwarmSpawner)

    token = user.new_api_token()
    # Start the server
    r = await api_request(app, "users", username, "servers", server_name, method="post")
    pending = r.status_code == 202
    while pending:
        # Request again
        r = await api_request(app, "users", username)
        user_info = r.json()
        pending = user_info["servers"][server_name]["pending"]

    assert r.status_code in {201, 200}, r.text
