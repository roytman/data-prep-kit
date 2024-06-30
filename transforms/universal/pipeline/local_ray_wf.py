from kfp import local
from kfp import dsl

local.init(runner=local.DockerRunner())

@dsl.component(base_image="quay.io/dataprep1/data-prep-kit/doc_id-ray:0.2.0")
def doc_id():
    import os
    import sys

    from data_processing.utils import ParamsUtils
    from data_processing_ray.runtime.ray import RayTransformLauncher
    from doc_id_transform_ray import DocIDRayTransformConfiguration

    # create parameters
    input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__),  "/home/dpk", "test-data/input"))
    output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__),  "/home/dpk", "output"))
    local_conf = {
        "input_folder": input_folder,
        "output_folder": output_folder,
    }
    worker_options = {"num_cpus": 0.8}
    code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
    params = {
        # where to run
        "run_locally": True,
        # Data access. Only required parameters are specified
        "data_local_config": ParamsUtils.convert_to_ast(local_conf),
        # orchestrator
        "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
        "runtime_num_workers": 2,
        "runtime_pipeline_id": "pipeline_id",
        "runtime_job_id": "job_id",
        "runtime_creation_delay": 0,
        "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
        # doc id configuration
        "doc_id_doc_column": "contents",
        "doc_id_hash_column": "hash_column",
        "doc_id_int_column": "int_id_column",
    }
    sys.argv = ParamsUtils.dict_to_req(d=params)
    # create launcher

    launcher = RayTransformLauncher(DocIDRayTransformConfiguration())
    # launch
    launcher.launch()

@dsl.component(base_image="quay.io/dataprep1/data-prep-kit/ededup-ray:0.2.0")
def ededup():
    import os
    import sys

    from data_processing.utils import ParamsUtils
    from data_processing_ray.runtime.ray import RayTransformLauncher
    from ededup_transform_ray import EdedupRayTransformConfiguration

    # create launcher
    launcher = RayTransformLauncher(EdedupRayTransformConfiguration())
    # create parameters
    input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__),  "/home/dpk", "test-data/input"))
    output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__),  "/home/dpk", "output"))
    local_conf = {
        "input_folder": input_folder,
        "output_folder": output_folder,
    }
    worker_options = {"num_cpus": 0.8}
    code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
    params = {
        # where to run
        "run_locally": True,
        # Data access. Only required parameters are specified
        "data_local_config": ParamsUtils.convert_to_ast(local_conf),
        # orchestrator
        "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
        "runtime_num_workers": 3,
        "runtime_pipeline_id": "pipeline_id",
        "runtime_job_id": "job_id",
        "runtime_creation_delay": 0,
        "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
        # ededup parameters
        "ededup_hash_cpu": 0.5,
        "ededup_num_hashes": 2,
        "ededup_doc_column": "contents",
    }
    sys.argv = ParamsUtils.dict_to_req(d=params)

    # launch
    launcher.launch()

@dsl.component(base_image="quay.io/dataprep1/data-prep-kit/fdedup-ray:0.2.0")
def fdedup():
    import os
    import sys

    from data_processing.utils import ParamsUtils
    from data_processing_ray.runtime.ray import RayTransformLauncher
    from fdedup_transform_ray import FdedupRayTransformConfiguration

    # create launcher
    launcher = RayTransformLauncher(FdedupRayTransformConfiguration())
    # create parameters
    input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "/home/dpk", "test-data/input"))
    output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "/home/dpk", "output"))
    local_conf = {
        "input_folder": input_folder,
        "output_folder": output_folder,
    }
    worker_options = {"num_cpus": 0.8}
    code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
    params = {
        # where to run
        "run_locally": True,
        # Data access. Only required parameters are specified
        "data_local_config": ParamsUtils.convert_to_ast(local_conf),
        # Orchestration parameters
        "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
        "runtime_num_workers": 1,
        "runtime_pipeline_id": "pipeline_id",
        "runtime_job_id": "job_id",
        "runtime_creation_delay": 0,
        "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
        # columns used
        "fdedup_doc_column": "contents",
        "fdedup_id_column": "int_id_column",
        "fdedup_cluster_column": "cluster",
        # infrastructure
        "fdedup_bucket_cpu": 0.5,
        "fdedup_doc_cpu": 0.5,
        "fdedup_mhash_cpu": 0.5,
        "fdedup_num_doc_actors": 1,
        "fdedup_num_bucket_actors": 1,
        "fdedup_num_minhash_actors": 1,
        "fdedup_num_preprocessors": 2,
        # fuzzy parameters
        "fdedup_num_permutations": 64,
        "fdedup_threshold": 0.8,
        "fdedup_shingles_size": 5,
        "fdedup_delimiters": " ",
        # Random delay between reads
        "fdedup_random_delay_limit": 5,
        # snapshotting
        "fdedup_snapshot_delay": 1,
        "fdedup_use_doc_snapshot": False,
        "fdedup_use_bucket_snapshot": False,
    }
    sys.argv = ParamsUtils.dict_to_req(d=params)

    # launch
    launcher.launch()

# or run it in a pipeline
@dsl.pipeline
def test_pipeline():
    doc_id()
    ededup().after(doc_id)
    fdedup().after(ededup)

pipeline_task = test_pipeline()