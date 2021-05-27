# mod-data-import

Copyright (C) 2018-2019 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.

<!-- ../../okapi/doc/md2toc -l 2 -h 4 README.md -->
* [Introduction](#introduction)
* [Compiling](#compiling)
* [Docker](#docker)
* [Installing the module](#installing-the-module)
* [Deploying the module](#deploying-the-module)

## Introduction

FOLIO data import module.

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
## Module properties to set up at mod-configuration

* **_data.import.storage.type_** - type of data storage used for uploaded files. Default value is **LOCAL_STORAGE**. Other implementations for storage should be added.
* **_data.import.storage.path_** - **path where uploaded file will be stored**

****In case of multiple instance deployment, for every instance the same persistent volume must be mounted to the mount point defined by the value of _data.import.storage.path_ property.****

## Interaction with Kafka


There are several properties that should be set for modules that interact with Kafka: **KAFKA_HOST, KAFKA_PORT, OKAPI_URL, ENV**(unique env ID).
After setup, it is good to check logs in all related modules for errors.

**Environment variables** that can be adjusted for this module and default values:
* "_file.processing.buffer.chunk.size_": 50

#### Note
**These variables are relevant for the **Iris** release. Module version: 2.0.0 (2.0.1, 2.0.2).**


## Maximum upload file size and java heap memory setups
mod-data-import provides the ability to uplaod a file of any size. The only limitation is related to the current implementation of the RMB and the size of the heap in the java process. Currently, before saving the file, it is read into memory, respectively, it is necessary to have the size of the java heap equal to the file size plus 10 percent.

### Example
| File Size | Java Heap size |
|:---------:|:--------------:|
|   256mb   |     270+ mb    |
|   512mb   |     560+ mb    |
|    1GB    |     1.1+ GB    |

## Issue tracker

See project [MODDATAIMP](https://issues.folio.org/browse/MODDATAIMP)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker/).

## Additional information

The [raml-module-builder](https://github.com/folio-org/raml-module-builder) framework.

Other [modules](https://dev.folio.org/source-code/#server-side).

See project [MODDATAIMP](https://issues.folio.org/browse/MODDATAIMP) at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker).

Other FOLIO Developer documentation is at [dev.folio.org](https://dev.folio.org/)

## Secret button
For now, file processing using UI can be initialized by secret button.
Secret button sends a request to /data-import/uploadDefinitions/{id}/processFile with hardcoded default job profile, 
which is being created in module converter-storage only if it is deployed in test mode.
Information about how to run module converter-storage in test mode can be find by reference 
in [Sample data section] (https://github.com/folio-org/mod-data-import-converter-storage#sample-data)

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
