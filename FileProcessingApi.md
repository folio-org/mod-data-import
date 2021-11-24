# File Processing API

## Introduction

This documentation describes API for the file processing. 
**mod-data-import** module performs processing for already uploaded files. For more information on file upload, please see [(documentation for file uploading)](FileUploadApi.md).
File processing implies dividing files into chunks of complete raw records and sending these chunks to another FOLIO modules for the further handling.

## Responsible entities and DTOs

### ProcessFilesRqDto

ProcessFilesRqDto describes incoming request with necessary information to perform file processing. ProcessFilesRqDto contains UploadDefinition entity (which is described [here](FileUploadApi.md)) and JobProfile entity (see below).

|Field | Description |
| ------ | ------ |
| uploadDefinition | Upload definition entity. |
| jobProfile | Job profile entity. |
All fields are required.

### JobProfile entity 

JobProfile carries information about profile type that identifies a way how to process target files.
Currently we do not choose the JobProfile and simply provide a stub one.

## File Processing API

**mod-data-import** module provides only one endpoint to start file processing.

| Method | URL | ContentType |Description |
| ------ |------ | ------ |------ |
| **POST** | /data-import/uploadDefinitions/{uploadDefinitionId}/processFiles  | application/json | Starts the file processing |

## File Processing Workflow

To initiate file processing send POST request containing UploadDefinition (with list of files) and JobProfile 
to follow path: /data-import/uploadDefinitions/{uploadDefinitionId}/processFiles.
```
curl -w '\n' -X POST -D - \
   -H "Content-type: application/json" \
   -H "Accept: text/plain, application/json"   \
   -H "x-okapi-tenant: diku"  \
   -H "x-okapi-token: eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkaWt1X2FkbWluIiwidXNlcl9pZCI6IjIzYWFmZDRjLTE1NmUtNTc0YS05Yjc1LWNkYTVmZmVlOWI3YyIsImNhY2hlX2tleSI6ImU4MmYwODc0LTI5NGEtNDc2ZS1hOTZhLTAxMDU2YWE1N2EzYSIsImlhdCI6MTU2MTAxNzUxNiwidGVuYW50IjoiZGlrdSJ9.h_IJSUrGJ2hK06St79vGpWPNwzczkBDUk6q4Y7p_9K0" \
   -d @processFilesRqDto.json \
   http://localhost:9130/data-import/uploadDefinitions/562ddb58-65ba-45a1-bf12-bf126fbb960d/processFiles
```

##### processFilesRqDto.json

```
{
  "uploadDefinition": {
    "id": "562ddb58-65ba-45a1-bf12-bf126fbb960d",
    "metaJobExecutionId": "09493c91-7ebd-4a61-bae4-222efaa6c583",
    "status": "LOADED",
    "createDate": "2019-06-20T11:00:25.852+0000",
    "fileDefinitions": [
      {
        "id": "ec33553d-b92a-45a8-8655-a4960129dd6e",
        "name": "CornellFOLIOExemplars.mrc",
        "status": "UPLOADED",
        "jobExecutionId": "09493c91-7ebd-4a61-bae4-222efaa6c583",
        "uploadDefinitionId": "562ddb58-65ba-45a1-bf12-bf126fbb960d",
        "createDate": "2019-06-20T11:00:25.852+0000"
      }
    ],
    "metadata": {
      "createdDate": "2019-06-20T11:00:25.828+0000",
      "createdByUserId": "23aafd4c-156e-574a-9b75-cda5ffee9b7c",
      "updatedDate": "2019-06-20T11:00:25.828+0000",
      "updatedByUserId": "23aafd4c-156e-574a-9b75-cda5ffee9b7c"
    }
  },
  "jobProfileInfo": {
    "id": "e34d7b92-9b83-11eb-a8b3-0242ac130003",
    "name": "Default - Create instance and SRS MARC Bib",
    "dataType": "MARC"
  }
}
```

##### Response

If the file processing is successfully initiated, there won't be any content in the response (HTTP status 204).
