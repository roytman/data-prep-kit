import subprocess
from typing import Any, List
import importlib
from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.utils import ParamsUtils
import sys

from data_processing.runtime.pure_python.runtime_configuration import (
    PythonTransformRuntimeConfiguration,
)
from virtual_env_updater import AbstractEnvUpdater, GitHubEnvUpdater, LocalRepositoryEnvUpdater

from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.utils import get_logger
import os

logger = get_logger(__name__)


class PipelineStep:

    def __init__(
            self,
            name: str,
            path_2_project_definition: str,
            package_name: str,
            module_name: str,
            transformer_config_class_name: str,
            params: dict[str, Any],
            updater: AbstractEnvUpdater = None,
    ):
        """
        Defines a pipeline processing step
        :param name: the step name
        :param updater: the virtual env updater that can be used for load dependencies for the step
        :param path_2_project_definition: path or URL to a project.toml file. This URL/path can be absolute or relative, depends on the updater definition.
        "param package_name: the name of the installed package
        :param module_name: transformer module name
        :param transformer_config_class_name: name of the transformer configuration class
        :param: params: transformer parameters
        """
        self.name = name
        self.updater = updater
        self.path_2_project_definition = path_2_project_definition
        self.package_name = package_name
        self.params = params
        self.module_name = module_name
        self.transformer_config_class_name = transformer_config_class_name


class Pipeline:

    def __init__(
            self,
            shared_updater: AbstractEnvUpdater,
            steps: List[PipelineStep],
    ):
        """
        Defines a pipeline class
        :param shared_updater: an updated that can be used by all pipeline steps, Individual steps can overwrite it.
        :param steps: the pipeline steps
        """
        self.shared_updater = shared_updater
        self.steps = steps

    # TODO add/insert/delete steps

    def lunch_pipeline(self):
        for step in self.steps:
            self.lunch_step(step)

    def lunch_step(self, step: PipelineStep):
        if step.updater is not None:
            step.updater.install(step.path_2_project_definition)
        elif self.shared_updater is not None:
            self.shared_updater.install(step.path_2_project_definition)
        else:
            logger.warning(f"There is no virtual environment updater for {step.name}")
        orig_arv = sys.argv
        sys.argv = ParamsUtils.dict_to_req(step.params)

        importlib.invalidate_caches()
        module = importlib.import_module(step.module_name)
        # Get the class from the module
        transform_configuration_class = getattr(module, step.transformer_config_class_name)

        # create launcher
        launcher = PythonTransformLauncher(runtime_config=transform_configuration_class())
        logger.info(f"Launching {step.name} transform")
        launcher.launch()

        sys.argv = orig_arv
        if step.updater is not None:
            step.updater.uninstall(step.package_name)
        elif self.shared_updater is not None:
            self.shared_updater.uninstall(step.package_name)
        importlib.invalidate_caches()

if __name__ == "__main__":
    # create NOOP parameters
    input_folder = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../transforms/universal/noop/python", "test-data", "input"))
    output_folder = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../transforms/universal/noop/python", "output"))
    local_conf = {
        "input_folder": input_folder,
        "output_folder": output_folder,
    }
    code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
    params = {
        # Data access. Only required parameters are specified
        "data_local_config": ParamsUtils.convert_to_ast(local_conf),
        # execution info
        "runtime_pipeline_id": "pipeline_id",
        "runtime_job_id": "job_id",
        "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
        # noop params
        "noop_sleep_sec": 1,
    }
    subprocess.check_call[sys.executable, "setup_execution.py", ParamsUtils.dict_to_req(d=params)],
    # noop_step = PipelineStep(name="noop", path_2_project_definition="transforms/universal/noop/python",
    #                         package_name="dpk_noop_transform_python", module_name="noop_transform_python",
    #                         transformer_config_class_name='NOOPPythonTransformConfiguration', params=params)
    #
    # updater = LocalRepositoryEnvUpdater("../../../")
    #
    # pipeline = Pipeline(shared_updater=updater, steps=[noop_step])

    # # Set the simulated command line args
    # print(f"sys_argv 1 = {sys.argv}")
    # sys.argv = ParamsUtils.dict_to_req(d=params)
    # print(f"sys_argv 2 = {sys.argv}")
    #
    # importlib.invalidate_caches()
    # updater.install("transforms/universal/noop/python")
    #
    # module_name = 'noop_transform_python'
    # class_name = 'NOOPPythonTransformConfiguration'
    # # from noop_transform_python import NOOPPythonTransformConfiguration
    # # Import the module
    #
    # module = importlib.import_module(module_name)
    # # Get the class from the module
    # NOOP_python_transform_configuration = getattr(module, class_name)
    #
    # # create launcher
    # launcher = PythonTransformLauncher(runtime_config=NOOP_python_transform_configuration())
    # # Launch the ray actor(s) to process the input
    # launcher.launch()
    # updater.uninstall("dpk_noop_transform_python")
    # importlib.invalidate_caches()
    # sys.argv = {}
    pipeline.lunch_pipeline()
    print(f"sys_argv 3 = {sys.argv}")
