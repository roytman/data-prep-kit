FROM docker.io/rayproject/ray:2.9.3-py310

ARG BUILD_DATE
ARG GIT_COMMIT
ARG DPK_LIB_VERSION
ARG DPK_LIB_KFP_VERSION

LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
LABEL data-prep-toolkit=${DPK_LIB_VERSION}
LABEL data-prep-toolkit-kfp=${DPK_LIB_KFP_VERSION}

# install libraries
COPY requirements.txt requirements.txt
RUN pip install kfp==2.7.0 --extra-index-url https://pypi.org/simple
RUN pip install kfp-kubernetes --extra-index-url https://pypi.org/simple
RUN pip install --no-cache-dir -r  requirements.txt
# remove credentials-containing file
RUN rm requirements.txt



COPY ./src /pipelines/component/src
