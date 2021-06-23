# osiris-sdk <!-- omit in toc -->

- [Installing](#installing)
- [Getting Started](#getting-started)
- [Data application registration](#data-application-registration)
  - [Prerequisites](#prerequisites)
  - [Steps](#steps)
    - [Create an App Registration](#create-an-app-registration)
    - [Create a Service Principal and credentials](#create-a-service-principal-and-credentials)
    - [Grant access to the dataset](#grant-access-to-the-dataset)
- [Usage](#usage)
  - [Upload](#upload)
  - [Download](#download)
  - [Time series pipeline](#time-series-pipeline)
  - [Data conversion pipeline](#data-conversion-pipeline)
  - [Ingress Adapter](#ingress-adapter)


## Installing

``` shell
$ pip install osiris-sdk
```
The SDK requires Python 3.

## Getting Started

To get started with the SDK you will need the URL to the Osiris-ingress API and the tenant ID for the 
organisation who runs the API. Furthermore, you will need to register your application withing the tenant 
using Azure App Registration. You will also need to create a dataset in the DataPlatform.

## Data application registration
An App Registration with credentials are required to upload data to the DataPlatform through the Osiris Ingress API.


### Prerequisites

* The dataset has been created through [the Data Platform](https://dataplatform.energinet.dk/).
* The Azure CLI is installed on your workstation

### Steps
Login with the Azure CLI with the following command:

``` bash
az login
```

You can also specify a username and password with:

``` bash
az login -u <username> -p <password>
```

#### Create an App Registration
The App Registration serves as a registration of trust for your application (or data publishing service) towards the Microsoft Identity Platform (allowing authentication).

This is the "identity" of your application.
Note that an App Registration is globally unique.

Run the following command:
``` bash
az ad app create --display-name "<YOUR APP NAME>"
```

The application name should be descriptive correlate to the application/service you intend to upload data with.

Take note of the `appId` GUID in the returned object.


#### Create a Service Principal and credentials
The Service Principal and credentials are what enables authorization to the Data Platform.

Create a Service Principal using the `appId` GUID from when creating the App Registration:
``` bash
az ad sp create --id "<appID>"
```

Then create a credential for the App Registration:

``` bash
az ad app credential reset --id "<appID>"
```

**NOTE:** Save the output somewhere secure. The credentials you receive are required to authenticate with the Osiris Ingress API.


#### Grant access to the dataset
The application must be granted read- and write-access to the dataset on [the Data Platform](https://dataplatform.energinet.dk/).

Add the application you created earlier, using the `<YOUR APP NAME>` name, to the read- and write-access lists.

## Usage
Here are some simple examples on how to use the SDK.

### Upload
The following is a simple example which shows how you can upload files using the Osiris SDK:
``` python
from osiris.apis.ingress import Ingress

ingress = Ingress(ingress_url=<INGRESS_URL>,
                  tenant_id=<TENANT_ID>,
                  client_id=<CLIENT_ID>,
                  client_secret=<CLIENT_SECRET>,
                  dataset_guid=<DATASET_GUID>)

file = open('test_file.json', 'rb')

# Without schema validation and a JSON file
ingress.upload_json_file(file, False)

# With schema validation and a JSON file
ingress.upload_json_file(file, True)

# With schema validation, a JSON file and event time
ingress.upload_json_file_event_time(file, '2021-01-01', True)

# Arbitrary file
ingress.upload_file(file)

# Save state file
with open('state.json', 'r') as state:
    ingress.save_state(state)

# Retrieve state
state = ingress.retrieve_state()
```

### Download
The following is a simple example which shows how you can download files using the Osiris SDK:
``` python
from osiris.apis.egress import Egress

egress = Egress(egress_url=<EGRESS_URL>,
                tenant_id=<TENANT_ID>,
                client_id=<CLIENT_ID>,
                client_secret=<CLIENT_SECRET>,
                dataset_guid=<DATASET_GUID>)

# JSON file
file_date: date = datetime.utcnow().date(),
content_json = egress.download_json_file(file_date)

# Arbitrary file
content_arbitrary = egress.download_file(file_date)
```

### Time series pipeline
The following is a simple example which shows how you can create a time series pipeline.
``` python
from osiris.pipelines.pipeline_timeseries import PipelineTimeSeries

pipeline = PipelineTimeSeries(storage_account_url=<AZURE_STORAGE_ACCOUNT_URL>,
                              filesystem_name=<CONTAINER_NAME>,
                              tenant_id=<TENANT_ID>,
                              client_id=<CLIENT_ID>,
                              client_secret=<CLIENT_SECRET>,
                              source_dataset_guid=<DATASET_GUID>,
                              destination_dataset_guid=<DATASET_GUID>,
                              date_format=<DATE_FORMAT_FOR_DATE_FIELD>,  # Example: "%Y-%m-%dT%H:%M:%S.%fZ"
                              date_key_name=<FIELD_NAME_FOR_EVENT_TIME>)

# Running the pipeline with current time
pipeline.transform_ingest_time_to_event_time_daily()

# Running the pipeline with specific time
ingest_time = datetime.datetime(2021, 04, 08, 12, 0, 0)  # April 4th, 2021 at 12:00:00
pipeline.transform_ingest_time_to_event_time_daily(ingest_time=ingest_time)
```

### Data conversion pipeline
This is an example of using the data conversion classes to transform structured data into other formats
of structured data.
``` python
from osiris.pipelines.pipeline_conversion import PipelineConversion

pipeline = PipelineConversion(storage_account_url=<AZURE_STORAGE_ACCOUNT_URL>,
                              filesystem_name=<CONTAINER_NAME>,
                              tenant_id=<TENANT_ID>,
                              client_id=<CLIENT_ID>,
                              client_secret=<CLIENT_SECRET>,
                              source_dataset_guid=<DATASET_GUID>,
                              destination_dataset_guid=<DATASET_GUID>)

# Running the pipeline with current time, using method defaults
pipeline.transform_convert_csv_to_json()

# Running the pipeline with specific time and tab as CSV separator
ingest_time = datetime.datetime(2021, 04, 08, 12, 0, 0)  # April 4th, 2021 at 12:00:00
pipeline.transform_convert_csv_to_json(ingest_time=ingest_time, separator='\t')
```

### Ingress Adapter
The following is a simple example which shows how you can create a new ingress adapter.
``` python
import json
from osiris.adapters.ingress_adapter import IngressAdapter


class MyAdapter(IngressAdapter):
    def retrieve_data(self) -> bytes:
        return json.dumps('Hello World').encode('UTF-8')


def main():
    adapter = MyAdapter(ingress_url=<INGRESS_URL>,
                        tenant_id=<TENANT_ID>,
                        client_id=<CLIENT_ID>,
                        client_secret=<CLIENT_SECRET>,
                        dataset_guid=<DATASET_GUID>)

    # as json data
    adapter.upload_json_data(schema_validate=False)
    
    # or as arbitrary data
    adapter.upload_data('bin')

if __name__ == '__main__':
    main()
```
