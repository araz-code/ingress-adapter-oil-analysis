# ingress-adapter-oil-analysis

## Introduction
This adapter takes an XLSX file as input and ingests it in an Ingress dataset in JSON format.

### Usage
```sh
$ python -m ingress_adapter_oil_analysis.adapter
```

## Configuration
The application needs a configuration file `conf.ini` (see `conf.example.ini`). 
The configuration file must be placed in the root of the project or in the locations `/etc/osiris/conf.ini` or 
`/etc/ingress-adapter-oil-analysis-conf.ini`. 

```
[Logging]
configuration_file = log.conf

[Azure Storage]
ingress_url = https://<storage_account>.dfs.core.windows.net

[Authorization]
tenant_id = <tenant_id>
client_id = <client_id>
client_secret = <client_secret>

[Datasets]
source = <source_guid>

[Excel]
file = <file>
sheet1 = <sheet1>
```

