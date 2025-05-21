FROM bitnami/spark

USER root

RUN python3 -m pip install scikit-learn && \
    python3 -m pip install pandas && \
    python3 -m pip install numpy

RUN install_packages curl

ENV PYSPARK_PYTHON python3
USER 1001