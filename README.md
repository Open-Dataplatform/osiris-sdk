# osiris-sdk <!-- omit in toc -->

- [Installing](#installing)
- [Getting started](#getting_started)
- [Data application registration](#data-application-registration)
  - [Prerequisites](#prerequisites)
  - [Steps](#steps)
    - [Create an App Registration](#create-an-app-registration)
    - [Create a Service Principal and credentials](#create-a-service-principal-and-credentials)
    - [Grant access to the dataset](#grant-access-to-the-dataset)
- [Usage](#usage)
  - [Upload](#upload)
  - [Download](#download)


## Installing

```shell
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
```
from osiris.ingress import Ingress

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

# Arbitrary file
ingress.upload_file(file)
```

### Download
The following is a simple example which shows how you can download files using the Osiris SDK:
```
from osiris.egress import Egress

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