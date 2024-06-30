
from data_processing.utils import ParamsUtils
from noop_transform_python import NOOPPythonTransformConfiguration

if __name__ == "__main__":
    sys.argv = ParamsUtils.dict_to_req(step.params)

    launcher = PythonTransformLauncher(runtime_config=NOOPPythonTransformConfiguration())
    launcher.launch()
    # if step.updater is not None:
    #     step.updater.uninstall(step.package_name)
    # elif self.shared_updater is not None:
    #     self.shared_updater.uninstall(step.package_name)
    # importlib.invalidate_caches()
