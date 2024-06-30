from kfp import local
from kfp import dsl

local.init(runner=local.DockerRunner())

@dsl.component(base_image="quay.io/dataprep1/data-prep-kit/doc_id-ray:0.2.0")
def doc_id():
    import subprocess
    import sys
    subprocess.run([sys.executable, "src/doc_id_local_ray.py"], check=True)

@dsl.component(base_image="quay.io/dataprep1/data-prep-kit/ededup-ray:0.2.0")
def ededup():
    import subprocess
    import sys
    subprocess.run([sys.executable, "src/ededup_local_ray.py"], check=True)

@dsl.component(base_image="quay.io/dataprep1/data-prep-kit/fdedup-ray:0.2.0")
def fdedup():
    import subprocess
    import sys
    subprocess.run([sys.executable, "src/fdedup_local_ray.py"], check=True)

@dsl.pipeline
def test_pipeline():
    doc_id()
    ededup().after(doc_id)
    fdedup().after(ededup)

pipeline_task = test_pipeline()