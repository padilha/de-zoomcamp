FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir
RUN mkdir /opt/prefect/flows

COPY parameterized_flow.py /opt/prefect/flows/parameterized_flow.py