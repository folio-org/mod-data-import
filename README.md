# mod-data-import

Copyright (C) 2018-2022 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.

<!-- ../../okapi/doc/md2toc -l 2 -h 4 README.md -->
- [Introduction](#introduction)
- [Compiling](#compiling)
- [Docker](#docker)
- [Installing the module](#installing-the-module)
- [Deploying the module](#deploying-the-module)
- [Maximum upload file size and java heap memory setups](#maximum-upload-file-size-and-java-heap-memory-setups)
    - [Example](#example)
- [Scalability](#scalability)
  - [Module properties to set up at mod-configuration](#module-properties-to-set-up-at-mod-configuration)
- [Interaction with AWS S3/Minio](#interaction-with-aws-s3minio)
- [Interaction with Kafka](#interaction-with-kafka)
- [Other system properties](#other-system-properties)
- [Issue tracker](#issue-tracker)
- [Additional information](#additional-information)
- [Script to upload a batch of MARC records](#script-to-upload-a-batch-of-marc-records)

## Introduction

mod-data-import is responsible for uploading files (see [documentation for file uploading](FileUploadApi.md)), initial handling and sending records for further processing (see [documentation for file processing](FileProcessingApi.md)).

## Compiling

```
   mvn install
```

See that it says "BUILD SUCCESS" near the end.

## Docker

Build the docker container with:

```
   docker build -t mod-data-import .
```

Test that it runs with:

```
   docker run -t -i -p 8081:8081 mod-data-import
```

## Installing the module

Follow the guide of
[Deploying Modules](https://github.com/folio-org/okapi/blob/master/doc/guide.md#example-1-deploying-and-using-a-simple-module)
sections of the Okapi Guide and Reference, which describe the process in detail.

First of all you need a running Okapi instance.
(Note that [specifying](../README.md#setting-things-up) an explicit 'okapiurl' might be needed.)

```
   cd .../okapi
   java -jar okapi-core/target/okapi-core-fat.jar dev
```

We need to declare the module to Okapi:

```
curl -w '\n' -X POST -D -   \
   -H "Content-type: application/json"   \
   -d @target/ModuleDescriptor.json \
   http://localhost:9130/_/proxy/modules
```

That ModuleDescriptor tells Okapi what the module is called, what services it
provides, and how to deploy it.

## Deploying the module

Next we need to deploy the module. There is a deployment descriptor in
`target/DeploymentDescriptor.json`. It tells Okapi to start the module on 'localhost'.

Deploy it via Okapi discovery:

```
curl -w '\n' -D - -s \
  -X POST \
  -H "Content-type: application/json" \
  -d @target/DeploymentDescriptor.json  \
  http://localhost:9130/_/discovery/modules
```

Then we need to enable the module for the tenant:

```
curl -w '\n' -X POST -D -   \
    -H "Content-type: application/json"   \
    -d @target/TenantModuleDescriptor.json \
    http://localhost:9130/_/proxy/tenants/<tenant_name>/modules
```

## Maximum upload file size and java heap memory setups

Current implementation supports only storing of the file in a LOCAL_STORAGE (file system of the module). It has a couple of implications:
1. the request for processing the file can be processed only by the same instance of the module, which prevents mod-data-import from scaling 
2. file size that can be uploaded is limited to the java heap memory allocated to the module.
It is necessary to have the size of the java heap equal to the expected max file size plus 10 percent.

#### Example
| File Size | Java Heap size |
|:---------:|:--------------:|
|   256mb   |     270+ mb    |
|   512mb   |     560+ mb    |
|    1GB    |     1.1+ GB    |

## Scalability

To initialise processing of a file user should choose a Job Profile - that information is crucial as it basically contains the instructions on what to do with the uploaded file. However, this process happens after file is uploaded and comes to mod-data-import as a separate request.
External storage is required to make mod-data-import scalable. Implementation of the module has the possibility to read the configuration settings from mod-configuration.
To allow multiple instance deployment, for every instance the same persistent volume must be mounted to the mount point defined by the value of ****_data.import.storage.path_ property.****

### Module properties to set up at mod-configuration

* **_data.import.storage.type_** - type of data storage used for uploaded files. Default value is **LOCAL_STORAGE**. Other implementations for storage should be added.
* **_data.import.storage.path_** - **path where uploaded file will be stored**

## File splitting configuration

The file-splitting process may be configured with the following environment variables:

| Name                      | Description                                                                |
|---------------------------|----------------------------------------------------------------------------|
| `RECORDS_PER_SPLIT_FILE`  | The maximum number of records to include in a single file; default is 1000 |

## Interaction with AWS S3/Minio

This module uses S3-compatible storage as part of the file upload process.  The following environment variables must be set with values for your S3-compatible storage (AWS S3, Minio Server):

| Name                    | Description                                                                |
|-------------------------|----------------------------------------------------------------------------|
| `AWS_URL`               | URL of S3-compatible storage, e.g. `http://127.0.0.1:9000/`                |
| `AWS_REGION`            | S3 region                                                                  |
| `AWS_BUCKET`            | Bucket to store and retrieve data                                          |
| `AWS_ACCESS_KEY_ID`     | S3 access key                                                              |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key                                                              |
| `AWS_SDK`               | If AWS S3 is being used (should be `"true"` if so and `"false"` otherwise) |
| `S3_FORCEPATHSTYLE`     | If path-style requests should be used instead of virtual-hosted-style      |

## Queue scoring algorithm

This covers the following environment variables:

* `SCORE_JOB_SMALLEST`
* `SCORE_JOB_LARGEST`
* `SCORE_JOB_REFERENCE`
* `SCORE_AGE_NEWEST`
* `SCORE_AGE_OLDEST`
* `SCORE_AGE_EXTREME_THRESHOLD_MINUTES`
* `SCORE_AGE_EXTREME_VALUE`
* `SCORE_TENANT_USAGE_MIN`
* `SCORE_TENANT_USAGE_MAX`
* `SCORE_PART_NUMBER_FIRST`
* `SCORE_PART_NUMBER_LAST`
* `SCORE_PART_NUMBER_LAST_REFERENCE`

For information on what these mean and how to configure them, please see [this wiki page](https://wiki.folio.org/display/FOLIOtips/Queue+and+chunk+processing+customization).

## Interaction with Kafka

All modules involved in data import (mod-data-import, mod-source-record-manager, mod-source-record-storage, mod-inventory, mod-invoice) are communicating via Kafka directly. Therefore, to enable data import Kafka should be set up properly and all the necessary parameters should be set for the modules.

**Properties that are required for mod-data-import to interact with Kafka:**

* `KAFKA_HOST`
* `KAFKA_PORT`
* `OKAPI_URL`
* `ENV` (unique env ID)

## Other system properties

Initial handling of the uploaded file means chunking it and sending records for processing in other modules. The chunk size can be adjusted for different files, otherwise default values will be used:

* "_file.processing.marc.raw.buffer.chunk.size_": 50 - applicable to MARC files in binary format
* "_file.processing.marc.json.buffer.chunk.size_": 50 - applicable to json files with MARC data in json format
* "_file.processing.marc.xml.buffer.chunk.size_": 10 - applicable to xml files with MARC data in xml format
* "_file.processing.edifact.buffer.chunk.size_": 10 - applicable to EDIFACT files

## Issue tracker

See project [MODDATAIMP](https://issues.folio.org/browse/MODDATAIMP)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker/).

## Additional information

The [raml-module-builder](https://github.com/folio-org/raml-module-builder) framework.

Other [modules](https://dev.folio.org/source-code/#server-side).

See project [MODDATAIMP](https://issues.folio.org/browse/MODDATAIMP) at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker).

Other FOLIO Developer documentation is at [dev.folio.org](https://dev.folio.org/)

## Script to upload a batch of MARC records

[The `scripts` directory](scripts) contains a shell-script, `load-marc-data-into-folio.sh`, and a file with a sample of 100 MARC records, `sample100.marc`. This script can be used to upload any batch of MARC files automatically, using the same sequence of WSAPI operations as the Secret Button. First, login to a FOLIO backend service using [the Okapi command-line utility](https://github.com/thefrontside/okapi.rb) or any other means that leaves definitions of the Okapi URL, tenant and token in the `.okapi` file in the home directory. Then run the script, naming the MARC file as its own argument:

```
scripts$ echo OKAPI_URL=https://folio-snapshot-stable-okapi.dev.folio.org > ~/.okapi
scripts$ echo OKAPI_TENANT=diku >> ~/.okapi
scripts$ okapi login
username: diku_admin
password: ************
Login successful. Token saved to /Users/mike/.okapi
scripts$ ./load-marc-data-into-folio.sh sample100.marc
=== Stage 1 ===
=== Stage 2 ===
=== Stage 3 ===
HTTP/2 204
date: Thu, 27 Aug 2020 11:55:28 GMT
x-okapi-trace: POST mod-authtoken-2.6.0-SNAPSHOT.73 http://10.36.1.38:9178/data-import/uploadDefinitions/123a8d01-e389-4893-a53e-cc2de846471d/processFiles.. : 202 7078us
x-okapi-trace: POST mod-data-import-1.11.0-SNAPSHOT.140 http://10.36.1.38:9175/data-import/uploadDefinitions/123a8d01-e389-4893-a53e-cc2de846471d/processFiles.. : 204 6354us
scripts$
```
