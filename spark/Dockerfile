# Spark oficial + pandas/pyarrow (necessários para applyInPandasWithState,
# usado no job stateful de carrinhos abandonados).
FROM apache/spark:3.5.5-python3
USER root
# Python 3.8 na imagem oficial -> pandas 2.0.3 é o máximo suportado.
RUN pip install --no-cache-dir "pandas==2.0.3" "pyarrow==15.0.2"
