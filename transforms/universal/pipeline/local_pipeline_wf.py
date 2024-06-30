# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from kfp import local
from kfp import dsl

local.init(runner=local.DockerRunner())

@dsl.component(base_image="quay.io/dataprep1/data-prep-kit/noop-python:0.2.0")
def noop():
    import subprocess
    import sys
    subprocess.run([sys.executable, "noop_transform_python.py"], check=True)

@dsl.component(base_image="quay.io/dataprep1/data-prep-kit/lang_id-ray:0.2.0")
def langID():
    import os

    from data_processing.data_access import DataAccessLocal
    from lang_id_transform import (
        LangIdentificationTransform,
        content_column_name_key,
        model_credential_key,
        model_kind_key,
        model_url_key,
    )
    from lang_models import KIND_FASTTEXT

    input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "/home/dpk", "test-data", "input"))

    lang_id_params = {
        model_credential_key: "PUT YOUR OWN HUGGINGFACE CREDENTIAL",
        model_kind_key: KIND_FASTTEXT,
        model_url_key: "facebook/fasttext-language-identification",
        content_column_name_key: "text",
    }
    transform = LangIdentificationTransform(lang_id_params)
    # Use the local data access to read a parquet table.
    data_access = DataAccessLocal()
    table = data_access.get_table(os.path.join(input_folder, "test_01.parquet"))
    print(f"input table: {table}")
    # Transform the table
    try:
        table_list, metadata = transform.transform(table[0])
        print(f"\noutput table: {table_list}")
        print(f"output metadata : {metadata}")
    except Exception as e:
        print(f"Exception executing transofm {e}")

@dsl.component(base_image="dataprep1/data-prep-kit/tokenization-python:0.2.0")
def tokenization():
    import os
    import sys

    from data_processing.runtime.pure_python import PythonTransformLauncher
    from data_processing.utils import ParamsUtils
    from tokenization_transform import TokenizationTransformConfiguration

    # create parameters
    input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "/home/dpk", "test-data", "ds01", "input"))
    output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "/home/dpk", "output", "ds01"))
    local_conf = {
        "input_folder": input_folder,
        "output_folder": output_folder,
    }
    worker_options = {"num_cpus": 0.8}
    code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
    params = {
        # Data access. Only required parameters are specified
        "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    }
    sys.argv = ParamsUtils.dict_to_req(d=params)
    # create launcher
    launcher = PythonTransformLauncher(TokenizationTransformConfiguration())
    # Launch the ray actor(s) to process the input
    launcher.launch()

@dsl.pipeline
def test_pipeline():
    noop()
    langID().after(noop)
    tokenization().after(langID)

pipeline_task = test_pipeline()