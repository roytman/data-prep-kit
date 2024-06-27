from data_processing.runtime.pure_python.runtime_configuration import (
    PythonTransformRuntimeConfiguration,
)
from virtual_env_updater import AbstractEnvUpdater
from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.utils import get_logger

logger = get_logger(__name__)


class PipelineStep:

    def __init__(
            self,
            name: str,
            updater: AbstractEnvUpdater,
            path_2_project_definition: str,
            package_name: str,
            config: PythonTransformRuntimeConfiguration,

    ):
        """
        Defines a pipeline processing step
        :param name: the step name
        :param updater: the virtual env updater that can be used for load dependencies for the step
        :param path_2_project_definition: path or URL to a project.toml file. This URL/path can be absolute or relative, depends on the updater definition.
        "param package_name: the name of the installed package
        :param: config: transformer configuration
        """


def lunch_step(step: PipelineStep):
    step.updater.install(step.path_2_project_definition)
    launcher = PythonTransformLauncher(step.config)
    logger.info(f"Launching {step.name} transform")
    launcher.launch()
    step.updater.uninstall(step.package_name)
