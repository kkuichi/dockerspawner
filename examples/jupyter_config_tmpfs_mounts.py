# Configuration file for jupyterhub.
import os

c = get_config()

c.JupyterHub.spawner_class = "jhub.SwarmSpawner"
c.JupyterHub.authenticator_class = "jhubauthenticators.DummyAuthenticator"
c.DummyAuthenticator.password = "password"

c.JupyterHub.ip = "0.0.0.0"
c.JupyterHub.hub_ip = "0.0.0.0"

c.JupyterHub.cleanup_servers = False

# First pulls can be really slow, so let's give it a big timeout
c.SwarmSpawner.start_timeout = 60 * 5

c.SwarmSpawner.jupyterhub_service_name = "jupyterhub"
c.SwarmSpawner.networks = ["jupyterhub_default"]

tmpfs_mounts = [
    {
        "type": "tmpfs",
        "source": "",
        "target": "/tmp/test_mount",
        "tmpfs_size": "1048576",
    }
]

c.SwarmSpawner.container_spec = {
    "command": "start-notebook.sh",
    "args": ["--NotebookApp.default_url=/lab"],
    "mounts": tmpfs_mounts,
}
