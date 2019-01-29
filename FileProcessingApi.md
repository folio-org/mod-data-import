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
The user has to choose JobProfile on UI and only then start file processing.

## File Processing API

**mod-data-import** module provides only one endpoint to start file processing.

| Method | URL | ContentType |Description |
| ------ |------ | ------ |------ |
| **POST** | /data-import/processFiles | application/json | Starts the file processing |

## File Processing Workflow

To initiate file processing send POST request containing UploadDefinition (with list of files) and JobProfile.
```
curl -w '\n' -X POST -D - \
   -H "Content-type: application/json" \
   -d @processFilesRqDto.json \
   http://localhost:9130/data-import/processFiles
```

##### processFilesRqDto.json

```
{
  "uploadDefinition": {
      "id":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
      "metaJobExecutionId":"99dfac11-1caf-4470-9ad1-d533f6360bdd",
      "status":"IN_PROGRESS",
      "fileDefinitions": [
          {
              "id":"88dfac11-1caf-4470-9ad1-d533f6360bdd",
              "uploadDefinitionId":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
              "jobExecutionId":"zx55tgh7-1caf-4470-9ad1-d533f6360bdd",
              "loaded":true,
              "name":"marc.mrc"
          }
      ]
  },
  "jobProfile": {
      "id": "zx5thml9-6hnq-45n0-23c0-13n8gbkl7091",
      "name": "Profile for marc files"
  }
}
```

##### Response

If the file processing is successfully initiated, there won't be any content in the response (HTTP status 204).
