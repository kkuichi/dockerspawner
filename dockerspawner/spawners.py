"""
A Spawner for JupyterHub that runs each user's server in a separate Docker Service
"""

from asyncio import sleep
from textwrap import dedent
from concurrent.futures import ThreadPoolExecutor
from pprint import pformat
from docker import DockerClient
from docker.errors import APIError, NotFound
from docker.tls import TLSConfig
from docker.types import (
    EndpointSpec,
    ServiceMode,
    Resources,
    RestartPolicy,
    UpdateConfig,
    RollbackConfig,
    Healthcheck,
    DNSConfig,
    Privileges,
    Mount,
    NetworkAttachmentConfig,
    SecretReference,
    ConfigReference,
    DriverConfig
)
from docker.utils import kwargs_from_env
from tornado import gen
from tornado.web import HTTPError
from jupyterhub.spawner import Spawner
from traitlets import default, Dict, List, Unicode
from flatten_dict import flatten, unflatten

class SwarmSpawner(Spawner):
    """
    A Spawner for JupyterHub using Docker Engine in Swarm mode
    """

    default_config = Dict(
        default_value= 
        {
            "image": "jupyterhub/singleuser:latest"
        },
        config=True,
        help=dedent(
            """
            Default service configuration.

            Check for more info:
            https://docker-py.readthedocs.io/en/stable/services.html
            """
        ),
    )

    profiles = List(
        trait=Dict(),
        default_value=[],
        config=True,
        help=dedent(
            """
            List of Docker configuration profiles available for the user.

            Profile is the dict with the `name` of the profile, `title`
            displayed in the html form and `config` dict: Docker service
            configuration.
            """
        ),
    )

    name_prefix = Unicode(
        "jupyter",
        config=True,
        help=dedent(
            """
            Prefix for service names.
            
            The full service name for a particular user will be
            <name_prefix>-<user_name>[-<server_name>].
            """
        ),
    )

    docker_client_tls_config = Dict(
        config=True,
        help=dedent(
            """
            Arguments to pass to docker TLS configuration.
            
            Check for more info:
            http://docker-py.readthedocs.io/en/stable/tls.html.
            """
        ),
    )

    form_template = Unicode(
        """
        <label for="profile">Select configuration:</label>
        <select class="form-control" name="profile" required autofocus>
            {option_template}
        </select>
        """,
        config=True,
        help="Form template.",
    )

    option_template = Unicode(
        """<option value="{name}" {selected}>{title}</option>""",
        config=True,
        help="Template for html form options.",
    )

    @default("port")
    def _port_default(self):
        return 8888

    # By default, do not inherit environment variables from the JupyterHub process
    @default("env_keep")
    def _env_keep(self):
        return []

    @default("options_form")
    def _options_form(self):
        if not self.profiles:
            return ""

        options = [
            self.option_template.format(
                name=prof["name"],
                title=prof.get("title", prof["name"]),
                selected=("selected" if i == 0 else ""))
            for i, prof in enumerate(self.profiles)
        ]

        form = self.form_template.format(option_template="".join(options))
        return form

    def options_from_form(self, form_data):
        if "profile" in form_data:
            selected = form_data["profile"][0]
            for prof in self.profiles:
                if prof["name"] == selected:
                    return { "user_profile": selected }
            raise HTTPError(
                400,
                "Docker profile with name {} not found".format(selected)
            )
        else:
            return {}

    service_id = Unicode()

    service_name = Unicode()

    @default("service_name")
    def _service_name(self):
        return (self.format_string("{prefix}-{username}-{servername}")
                if self.name else
                self.format_string("{prefix}-{username}"))

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
            cls._client = DockerClient(version="auto", **kwargs)

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

    def _docker(self, method, *args, **kwargs):
        """
        Wrapper for calling docker methods to be passed to ThreadPoolExecutor
        """
        m = self.client
        for attr in method.split("."):
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
            "Getting Docker service {}".format(self.service_name)
        )
        try:
            service = yield self.docker("services.get", self.service_name)
            self.service_id = service.id
        except NotFound:
                self.log.info("Docker service {} not found".format(self.service_name))
                service = None
                # Docker service is gone, remove service id
                self.service_id = ""
        except APIError as err:
            if err.is_server_error():
                self.log.info("Docker Swarm Server error")
                service = None
                # Docker service is unhealthy, remove the service_id
                self.service_id = ""
            else:
                raise err
        return service

    def _format_param(self, config, param):
        val = config.get(param)
        if val:
            if isinstance(val, str):
                config[param] = self.format_string(param)
            elif isinstance(val, list):
                config[param] = [self.format_string(elm) for elm in val]

    def _format_mount(self, mount):
        if isinstance(mount, str):
            return self.format_string(mount)
        elif isinstance(mount, dict):
            mount["target"] = self.format_string(mount["target"])
            mount["source"] = self.format_string(mount["source"])
            return mount

    def _format_config(self, config):
        self._format_param(config, "command")
        self._format_param(config, "args")
        self._format_param(config, "env")
        self._format_param(config, "user")
        self._format_param(config, "workdir")

        mounts = config.get("mounts")
        if mounts:
            config["mounts"] = [self._format_mount(mount) for mount in mounts]

        configs = config.get("configs")
        if configs:
            for conf in configs:
                self._format_param(conf, "filename")

        secrets = config.get("secrets")
        if secrets:
            for secr in secrets:
                self._format_param(secr, "filename")

        labels = config.get("labels")
        if labels:
            for lab, val in labels.items():
                labels[lab] = self.format_string(val)

        container_labels = config.get("container_labels")
        if container_labels:
            for lab, val in container_labels.items():
                container_labels[lab] = self.format_string(val)

        return config

    def get_service_config(self):
        config = {}

        config["name"] = self.service_name
        config["command"] = self.cmd
        config["args"] = self.get_args()
        config["env"] = ["{}={}".format(k, v) for k, v in self.get_env().items()]

        resources = {}
        if self.cpu_limit:
            resources["cpu_limit"] = self.cpu_limit * 10e9
        if self.mem_limit:
            mem = self.mem_limit
            resources["mem_limit"] = mem.lower() if isinstance(mem, str) else mem
        if self.cpu_guarantee:
            resources["cpu_reservation"] = self.cpu_guarantee * 10e9
        if self.mem_guarantee:
            mem = self.mem_guarantee
            resources["mem_reservation"] = mem.lower() if isinstance(mem, str) else mem
        if resources:
            config["resources"] = resources

        if self.default_config:
            config = _update_config(config, self.default_config)

        user_profile = self.user_options.get("user_profile", "")
        if user_profile:
            for prof in self.profiles:
                if user_profile == prof.get("name"):
                    profile_config = prof.get("config", {})
                    config = _update_config(config, profile_config)

        image = config["image"]
        config["env"].append("JUPYTER_IMAGE_SPEC={}".format(image))

        labels = {
            "org.jupyterhub.user": self.user.name,
            "org.jupyterhub.server": self.name,
            "org.jupyterhub.profile": user_profile
        }
        if "labels" in config:
            config["labels"].update(labels)
        else:
            config["labels"] = dict(labels)
        if "container_labels" in config:
            config["container_labels"].update(labels)
        else:
            config["container_labels"] = dict(labels)

        config = self._format_config(config)

        self.log.debug(
            "Config for service {} with id {}: {}".format(
                self.service_name, self.service_id[:7], pformat(config)
            )
        )

        return config

    @gen.coroutine
    def start(self):
        """
        Start the single-user server in a docker service.
        You can specify the params for the service through
        jupyterhub_config.py or using the user_options.
        """
        service = yield self.get_service()

        if service is None:
            self.log.info(
                "Creating Docker service for user {}".format(
                    self.user.name
                )
            )
            config = self.get_service_config()
            config = _parse_config(config)
            try:
                service = yield self.docker("services.create", **config)
                self.service_id = service.id
            except APIError as err:
                self.log.error(
                    "Error creating Docker service {} with config: {}".format(
                        self.service_name, pformat(config)
                    )
                )
                raise err
            self.log.info(
                "Created Docker service {} with id {} from image {} for user {}".format(
                    self.service_name, self.service_id[:7], config["image"], self.user.name
                )
            )
            yield self.wait_for_running_tasks()
        else:
            self.log.info(
                "Found existing Docker service {} with id {}".format(
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

        # We use service_name instead of ip
        # https://docs.docker.com/engine/swarm/networking/#use-swarm-mode-service-discovery
        # port should be default (8888)
        ip = self.service_name
        port = self.port

        return ip, port

    @gen.coroutine
    def stop(self, now=False):
        """
        Stop and remove the service
        Consider using stop/start when Docker adds support
        """
        self.log.info(
            "Stopping and removing Docker service {} with id {}".format(
                self.service_name, self.service_id[:7]
            )
        )

        try:
            result = yield self.docker("api.remove_service", self.service_name)
            # Even though it returns the service is gone
            # the underlying containers are still being removed
            if result:
                self.log.info(
                    "Docker service {} with id {} was removed".format(
                        self.service_name, self.service_id[:7]
                    )
                )
        except NotFound:
            self.log.warn("Docker service {} not found", self.service_name)
        except APIError:
            self.log.error("Error removing Docker service {} with id {}".format(
                self.service_name, self.service_id
            ))

        self.clear_state()

    @gen.coroutine
    def poll(self):
        """Check for a task state like `docker service ps id`"""

        try:
            tasks = yield self.docker("api.tasks", {"service": self.service_name})
        except NotFound:
            self.log.warn("Docker service {} not found", self.service_name)
            return 0
        if not tasks:
            self.log.warn("Tasks for service {} not found", self.service_name)
            return 0

        running_task = None
        for task in tasks:
            task_state = task["Status"]["State"]
            if task_state == "running":
                self.log.debug(
                    "Task {} of service with id {} status: {}".format(
                        task["ID"][:7], self.service_id[:7], pformat(task_state)
                    )
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
                # If the task is rejected -> remove service
                yield self.stop()

        if running_task is not None:
            return None
        else:
            return 0

    @gen.coroutine
    def wait_for_running_tasks(self, max_attempts=20):
        preparing, running = False, False
        attempt = 0
        while not running:
            tasks = yield self.docker("api.tasks", {"service": self.service_name})
            preparing = False
            for task in tasks:
                task_state = task["Status"]["State"]
                self.log.info(
                    "Waiting for service {}, current task status: {}".format(
                        self.service_name, task_state
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

    def template_namespace(self):
        ns = super().template_namespace()
        ns["prefix"] = self.name_prefix
        if self.name:
            ns["servername"] = self.name
        profile = self.user_options.get("user_profile", "")
        if profile:
            ns["profile"] = profile
        return ns

_SERVICE_TYPES = {
    "endpoints": EndpointSpec,
    "mode": ServiceMode,
    "mounts": Mount,
    "networks": NetworkAttachmentConfig,
    "resources": Resources,
    "restart_policy": RestartPolicy,
    "secrets": SecretReference,
    "update_config": UpdateConfig,
    "roleback_config": RollbackConfig,
    "healthcheck": Healthcheck,
    "dns_config": DNSConfig,
    "configs": ConfigReference,
    "privileges": Privileges
}

_MOUNT_TYPES = {
    "driver_config": DriverConfig
}

def _parse_obj(obj, types):
    for opt, val in obj.items():
        opt_type = types.get(opt)
        if opt_type:
            if isinstance(val, dict):
                obj[opt] = opt_type(**val)
            elif isinstance(val, list):
                obj[opt] = [opt_type(**elm) if isinstance(elm, dict) else elm
                           for elm in val]
    return obj

def _parse_config(config):
    mounts = config.get("mounts")
    if mounts:
        config["mounts"] = [_parse_obj(mount, _MOUNT_TYPES) for mount in mounts]
    config = _parse_obj(config, _SERVICE_TYPES)
    return config

def _update_config(config, update):
    config = flatten(config)
    update = flatten(update)

    for opt, val2 in update.items():
        if isinstance(val2, list) and opt in config:
            val1 = config[opt]
            assert isinstance(val1, list)
            config[opt] = val1 + val2 # Append update for lists
        else:
            config[opt] = val2

    return unflatten(config)
