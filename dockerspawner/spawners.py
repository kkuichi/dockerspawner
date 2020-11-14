"""
A Spawner for JupyterHub that runs each user's server in a separate Docker Service
"""

import os
import hashlib
import docker
import copy
from asyncio import sleep
from async_generator import async_generator, yield_
from textwrap import dedent
from concurrent.futures import ThreadPoolExecutor
from pprint import pformat
from docker.errors import APIError, NotFound
from docker.tls import TLSConfig
from docker.types import (
    TaskTemplate,
    Resources,
    ContainerSpec,
    DriverConfig,
    Placement,
    ConfigReference,
    EndpointSpec,
)
from docker.utils import kwargs_from_env
from tornado import gen
from jupyterhub.spawner import Spawner
from traitlets import default, Dict, Unicode, List, Bool, Int
from dockerspawner.mount import VolumeMounter
from dockerspawner.util import recursive_format

class UnicodeOrFalse(Unicode):
    info_text = "a unicode string or False"

    def validate(self, obj, value):
        if not value:
            return value
        return super(UnicodeOrFalse, self).validate(obj, value)

class SwarmSpawner(Spawner):
    """
    A Spawner for JupyterHub using Docker Engine in Swarm mode
    Makes a list of docker images available for the user to spawn
    Specify in the jupyterhub configuration file which are allowed:
    e.g.

    c.JupyterHub.spawner_class = 'jhub.SwarmSpawner'
    # Available docker images the user can spawn
    c.SwarmSpawner.dockerimages = [
        {'image': 'jupyterhub/singleuser:latest',
        'name': 'Default Jupyter notebook'}

    ]

    The images must be locally available before the user can spawn them
    """

    dockerimages = List(
        trait=Dict(),
        default_value=[
            {
                "image": "jupyterhub/singleuser:latest",
                "name": "Default Jupyter notebook",
            }
        ],
        minlen=1,
        config=True,
        help="Docker images that are available to the user of the host",
    )

    form_template = Unicode(
        """
        <label for="dockerimage">Select a notebook image:</label>
        <select class="form-control" name="dockerimage" required autofocus>
            {option_template}
        </select>""",
        config=True,
        help="Form template.",
    )

    option_template = Unicode(
        """<option value="{image}" {selected}>{name}</option>""",
        config=True,
        help="Template for html form options.",
    )

    @default("options_form")
    def _options_form(self):
        # TODO: return option html form
        return ""

    def options_from_form(self, form_data):
        """Parse the submitted form data and turn it into the correct
        structures for self.user_options."""
        # TODO: select options from specs, this will be stored in self.user_options
        return options

    service_name_prefix = Unicode(
        "jupyter",
        config=True,
        help=dedent(
            """
            Prefix for service names. The full service name for a particular
            user will be <prefix>-<hash(username)>-<server_name>.
            """
        ),
    )

    docker_client_tls_config = Dict(
        config=True,
        help=dedent(
            """Arguments to pass to docker TLS configuration.
            Check for more info:
            http://docker-py.readthedocs.io/en/stable/tls.html
            """
        ),
    )

    service_id = Unicode()

    service_port = Int(8888, min=1, max=65535, config=True)

    _service_owner = None

    @property
    def service_owner(self):
        if self._service_owner is None:
            m = hashlib.md5()
            m.update(self.user.name.encode("utf-8"))
            if hasattr(self.user, "real_name"):
                self._service_owner = self.user.real_name[-32:]
            elif hasattr(self.user, "name"):
                # Maximum 63 characters, 10 are comes from the underlying format
                # i.e. prefix=jupyter-, postfix=-1
                # get up to last 32 characters as service identifier
                self._service_owner = self.user.name[-32:]
            else:
                self._service_owner = m.hexdigest()
        return self._service_owner

    @property
    def service_name(self):
        """
        Service name inside the Docker Swarm
        """
        if self.name:
            return "{}-{}-{}".format(self.service_name_prefix, self.service_owner, server_name)
        else:
            return "{}-{}".format(self.service_name_prefix, self.service_owner)

    @property
    def tasks(self):
        return self._tasks

    @tasks.setter
    def tasks(self, tasks):
        self._tasks = tasks

    _executor = None

    @property
    def executor(self, max_workers=1):
        """Single global executor"""
        cls = self.__class__
        if cls._executor is None:
            cls._executor = ThreadPoolExecutor(max_workers)
        return cls._executor

    _client = None

    @property
    def client(self):
        """Single global client instance"""
        cls = self.__class__

        if cls._client is None:
            kwargs = {}
            if self.docker_client_tls_config:
                kwargs["tls"] = TLSConfig(**self.docker_client_tls_config)
            kwargs.update(kwargs_from_env())
            client = docker.APIClient(version="auto", **kwargs)

            cls._client = client
        return cls._client

    def load_state(self, state):
        super().load_state(state)
        self.service_id = state.get("service_id", "")

    def get_state(self):
        state = super().get_state()
        if self.service_id:
            state["service_id"] = self.service_id
        return state

    def clear_state(self):
        super().clear_state()
        self.service_id = ""

    def _public_hub_api_url(self):
        proto, path = self.hub.api_url.split("://", 1)
        _, rest = path.split(":", 1)
        return "{proto}://{name}:{rest}".format(
            proto=proto, name=self.jupyterhub_service_name, rest=rest
        )

    def get_env(self):
        env = super().get_env()
        return env

    def _docker(self, method, *args, **kwargs):
        """
        Wrapper for calling docker methods to be passed to ThreadPoolExecutor
        """
        m = self.client
        for attr in method.split('.')
            m = getattr(m, attr)
        return m(*args, **kwargs)

    def docker(self, method, *args, **kwargs):
        """
        Call a docker method in a background thread returns a Future
        """
        return self.executor.submit(self._docker, method, *args, **kwargs)

    @gen.coroutine
    def get_service(self):
        self.log.debug(
            "Getting Docker service '{}' with id: '{}'".format(
                self.service_name, self.service_id
            )
        )
        try:
            service = yield self.docker("services.get", self.service_name)
            self.service_id = service.id
        except NotFound:
                self.log.info("Docker service '{}' is gone".format(self.service_name))
                service = None
                # Docker service is gone, remove service id
                self.service_id = ""
        except APIError as err:
            if err.response.status_code == 500:
                self.log.info("Docker Swarm Server error")
                service = None
                # Docker service is unhealthy, remove the service_id
                self.service_id = ""
            else:
                raise
        return service

    @gen.coroutine
    def start(self):
        """
        Start the single-user server in a docker service.
        You can specify the params for the service through
        jupyterhub_config.py or using the user_options
        """
        self.log.info("User: {}, start spawn".format(self.user.__dict__))

        # https://github.com/jupyterhub/jupyterhub/blob/master/jupyterhub/user.py#L202
        # By default jupyterhub calls the spawner passing user_options
        if self.use_user_options:
            user_options = self.user_options
        else:
            user_options = {}

        service = yield self.get_service()
        if service is None:
            # TODO: prepare configuration

            # TODO: create service
            service = None
            self.service_id = service.id
            self.log.info(
                "Created Docker service {} (id: {}) from image {}"
                " for user {}".format(
                    self.service_name, self.service_id[:7], image, self.user
                )
            )

            yield self.wait_for_running_tasks()

        else:
            self.log.info(
                "Found existing Docker service {} (id: {})".format(
                    self.service_name, self.service_id[:7]
                )
            )
            # Handle re-using API token.
            # Get the API token from the environment variables
            # of the running service:
            envs = service.attrs["Spec"]["TaskTemplate"]["ContainerSpec"]["Env"]
            for line in envs:
                if line.startswith("JPY_API_TOKEN="):
                    self.api_token = line.split("=", 1)[1]
                    break

        ip = self.service_name
        port = self.service_port
        self.log.debug(
            "Active service: '{}' with user '{}'".format(self.service_name, self.user)
        )

        # We use service_name instead of ip
        # https://docs.docker.com/engine/swarm/networking/#use-swarm-mode-service-discovery
        # service_port is actually equal to 8888
        return ip, port

    @gen.coroutine
    def stop(self, now=False):
        """
        Stop and remove the service
        Consider using stop/start when Docker adds support
        """
        self.log.info(
            "Stopping and removing Docker service {} (id: {})".format(
                self.service_name, self.service_id[:7]
            )
        )

        service = yield self.get_service()
        if not service:
            self.log.warn("Docker service not found")
            return

        try:
            service.remove()
            # Even though it returns the service is gone
            # the underlying containers are still being removed
            self.log.info(
                "Docker service {} (id: {}) removed".format(
                    self.service_name, self.service_id[:7]
                )
            )
        except APIError:
            self.log.error("Error removing service {} (id: {})".format(
                self.server_name, self.service_id
            ))

        self.clear_state()

    @gen.coroutine
    def poll(self):
        """Check for a task state like `docker service ps id`"""
        service = yield self.get_service()
        if service is None:
            self.log.warn("Docker service not found")
            return 0

        running_task = None
        for task in service.tasks():
            task_state = task["Status"]["State"]
            if task_state == "running":
                self.log.debug(
                    "Task {} of service with id {} status: {}".format(
                        task["ID"][:7], self.service_id[:7], pformat(task_state)
                    ),
                )
                # There should be at most one running task
                running_task = task
            if task_state == "rejected":
                task_err = task["Status"]["Err"]
                self.log.error(
                    "Task {} of service with id {} status: {} message: {}".format(
                        task["ID"][:7],
                        self.service_id[:7],
                        pformat(task_state),
                        pformat(task_err),
                    )
                )
                # If the tasks is rejected -> remove it
                yield self.stop()

        if running_task is not None:
            return None
        else:
            return 0

    async def check_update(self, image, tag="latest"):
        full_image = "".join([image, ":", tag])
        download_tracking = {}
        initial_output = False
        total_download = 0
        for download in self.client.pull(image, tag=tag, stream=True, decode=True):
            if not initial_output:
                await yield_(
                    {
                        "progress": 70,
                        "message": "Downloading new update "
                        "for {}".format(full_image),
                    }
                )
                initial_output = True
            if "id" and "progress" in download:
                _id = download["id"]
                if _id not in download_tracking:
                    del download["id"]
                    download_tracking[_id] = download
                else:
                    download_tracking[_id].update(download)

                # Output every 20 MB
                for _id, tracker in download_tracking.items():
                    if (
                        tracker["progressDetail"]["current"]
                        == tracker["progressDetail"]["total"]
                    ):
                        total_download += tracker["progressDetail"]["total"] * pow(
                            10, -6
                        )
                        await yield_(
                            {
                                "progress": 80,
                                "message": "Downloaded {} MB of {}".format(
                                    total_download, full_image
                                ),
                            }
                        )
                        # return to web processing
                        await sleep(1)

                # Remove completed
                download_tracking = {
                    _id: tracker
                    for _id, tracker in download_tracking.items()
                    if tracker["progressDetail"]["current"]
                    != tracker["progressDetail"]["total"]
                }

    @async_generator
    async def progress(self):
        top_task = self.tasks[0]
        image = top_task["Spec"]["ContainerSpec"]["Image"]
        self.log.info("Spawning progress of {} with image".format(self.service_id))
        task_status = top_task["Status"]["State"]
        _tag = None
        if ":" in image:
            _image, _tag = image.split(":")
        else:
            _image = image
        if task_status == "preparing":
            await yield_(
                {
                    "progress": 50,
                    "message": "Preparing a server with the {} image".format(image),
                }
            )
            await yield_(
                {
                    "progress": 60,
                    "message": "Checking for new version of {}".format(image),
                }
            )
            if _tag is not None:
                await self.check_update(_image, _tag)
            else:
                await self.check_update(_image)
            self.log.info("Finished progress from spawning {}".format(image))

    @gen.coroutine
    def wait_for_running_tasks(self, max_attempts=20):
        preparing, running = False, False
        attempt = 0
        while not running:
            service = yield self.get_service()
            self.tasks = service.tasks()
            preparing = False
            for task in self.tasks:
                task_state = task["Status"]["State"]
                self.log.info(
                    "Waiting for service: {} current task status: {}".format(
                        service["ID"], task_state
                    )
                )
                if task_state == "running":
                    running = True
                if task_state == "preparing":
                    preparing = True
                if task_state == "rejected" or attempt > max_attempts:
                    return False
            if not preparing:
                attempt += 1
            yield gen.sleep(1)
