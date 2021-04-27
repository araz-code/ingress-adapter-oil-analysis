FROM python:3.8

COPY ./ingress_adapter_oil_analysis ./ingress_adapter_oil_analysis
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt