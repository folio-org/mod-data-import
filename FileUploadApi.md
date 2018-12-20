# File Upload API

## Introduction

This API is responsible for uploading files to storage. 
**mod-data-import** module use uploaded files for parsing and importing files to storage and another FOLIO modules usage.
File Upload API provides a ability to upload several files and store state of uploaded files. 
A user can delete or add another one file from uploading session.

## Entities

### UploadDefinition

Upload Definition entity describes an upload process and contains a list of file definitions.
Each file upload session process starts from creating UploadDefinition.

| Field | Description |
| ------ | ------ |
| id | UUID for an UploadDefinition entity |
| metaJobExecutionId | UUID of JobExecution process for all linked files. Specific for data-import application. |
| status | Show the current state of the upload process. Can be one of: "NEW", "IN_PROGRESS", "LOADED", "COMPLETED" |
| fileDefinitions | Array of the FileDefinition entities. Each FileDefinition describes particular state of the file |
| metadata | Standard field with metainformation about entity |

### FileDefinition

File Definition entity describes an upload process of particular file and store this process's state.

| Field | Description |
| ------ | ------ |
| id | UUID for an FileDefinition entity |
| name | Name of the file with its extension. This  field is required. |
| uploadDefinitionId | UUID of UploadDefinition related to this file |
| loaded | Boolean value that shows that a particular file already loaded and saved into the storage |
| jobExecutionId | UUID of the file's JobExecution process. Specific for data-import application. |
| sourcePath | The path to the file location in the storage. Readonly |
| createDate | Date and time when the file definition was created |

## File Upload API

For all necessary file upload lifecycle operations mod-data-import provides following endpoints: 

| Method | URL | ContentType |Description |
| ------ |------ | ------ |------ |
| **POST** | /data-import/upload/definition | application/json | Create new UploadDefinition |
| **GET** | /data-import/upload/definition | | Get list of UploadDefinitions by query |
| **GET** | /data-import/upload/definition/**{uploadDefinitionId}** | | Get single definition by UUID |
| **PUT** | /data-import/upload/definition/**{uploadDefinitionId}** | application/json | Update definition by UUID |
| **POST** | /data-import/upload/definition/file | application/json | Add new FileDefinition to UploadDefinition |
| **DELETE** | /data-import/upload/definition/file/**{fileDefinitionId}**?uploadDefinitionId=**{uploadDefinitionId}**  | | Delete FileDefinition from UploadDefinition by UUID |
| **POST** | /data-import/upload/file?uploadDefinitionId=**{uploadDefinitionId}**&fileId=**{fileDefinitionId}**  | application/octet-stream | Upload file data to backend storage. Need to set query params: uploadDefinitionId and fileDefinitionId |

## File Upload Workflow

### Create UploadDefinition

Each file uploading process starts from creating new UploadDefinition. 
Send POST request with list of file names for creating new UploadDefinition.

```
curl -w '\n' -X POST -D -   \
   -H "Content-type: application/json"   \
   -d @uploadDefinitionRequest.json \
   http://localhost:9130/data-import/upload/definition
```

##### uploadDefinitionRequest.json

```
{  
   "fileDefinitions":[  
      {  
         "name":"marc.mrc"
      },
      {  
         "name":"marc2.mrc"
      }
   ]
}
```

##### Response with UploadDefinition

```
{  
   "id":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
   "metaJobExecutionId":"99dfac11-1caf-4470-9ad1-d533f6360bdd",
   "status":"NEW",
   "fileDefinitions":[  
      {  
         "id":"88dfac11-1caf-4470-9ad1-d533f6360bdd",
         "uploadDefinitionId":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
         "jobExecutionId":"66dfac11-1caf-4470-9ad1-d533f6360bdd",
         "loaded":false,
         "name":"marc.mrc"
      },
      {  
         "id":"77dfac11-1caf-4470-9ad1-d533f6360bdd",
         "uploadDefinitionId":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
         "jobExecutionId":"55dfac11-1caf-4470-9ad1-d533f6360bdd",
         "loaded":false,
         "name":"marc2.mrc"
      }
   ]
}
```

### Upload file body to the backend
After UploadDefinition successfully created, backend ready to file uploading. 
For each file send a request with file's data. Content-type: application/octet-stream and body of request should be a file.

```
curl -w '\n' -X POST -D -   \
   -H "Content-type: application/octet-stream"   \
   -d @marc.mrc \
   http://localhost:9130/data-import/upload/file?uploadDefinitionId=67dfac11-1caf-4470-9ad1-d533f6360bdd&fileId=88dfac11-1caf-4470-9ad1-d533f6360bdd
```

##### Response with changed UploadDefinition

If the first file is loaded, UploadDefinition change status to "IN_PROGRESS" and "loaded" field of particular FileDefinition will be changed on "true" value.

```
{  
   "id":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
   "metaJobExecutionId":"99dfac11-1caf-4470-9ad1-d533f6360bdd",
   "status":"IN_PROGRESS",
   "fileDefinitions":[  
      {  
         "id":"88dfac11-1caf-4470-9ad1-d533f6360bdd",
         "uploadDefinitionId":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
         "jobExecutionId":"66dfac11-1caf-4470-9ad1-d533f6360bdd",
         "loaded":true,
         "name":"marc.mrc"
      },
      {  
         "id":"77dfac11-1caf-4470-9ad1-d533f6360bdd",
         "uploadDefinitionId":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
         "jobExecutionId":"55dfac11-1caf-4470-9ad1-d533f6360bdd",
         "loaded":false,
         "name":"marc2.mrc"
      }
   ]
}
```

##### Upload last file of the UploadDefinition

```
curl -w '\n' -X POST -D -   \
   -H "Content-type: application/octet-stream"   \
   -d @marc2.mrc \
   http://localhost:9130/data-import/upload/file?uploadDefinitionId=67dfac11-1caf-4470-9ad1-d533f6360bdd&fileId=77dfac11-1caf-4470-9ad1-d533f6360bdd
```

##### Response with changed UploadDefinition

As far as last file will be loaded, UploadDefinition change status to "LOADED"
```
{  
   "id":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
   "metaJobExecutionId":"99dfac11-1caf-4470-9ad1-d533f6360bdd",
   "status":"LOADED",
   "fileDefinitions":[  
      {  
         "id":"88dfac11-1caf-4470-9ad1-d533f6360bdd",
         "uploadDefinitionId":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
         "jobExecutionId":"66dfac11-1caf-4470-9ad1-d533f6360bdd",
         "loaded":true,
         "name":"marc.mrc"
      },
      {  
         "id":"77dfac11-1caf-4470-9ad1-d533f6360bdd",
         "uploadDefinitionId":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
         "jobExecutionId":"55dfac11-1caf-4470-9ad1-d533f6360bdd",
         "loaded":true,
         "name":"marc2.mrc"
      }
   ]
}
```

### Delete FileDefinition

##### Request for deleting second file

```
curl -X DELETE -D - -w '\n' http://localhost:9130/data-import/upload/definition/file/77dfac11-1caf-4470-9ad1-d533f6360bdd?uploadDefinitionId=67dfac11-1caf-4470-9ad1-d533f6360bdd
```

##### Response for deleting second file will be 204

UploadDefinition entity will be changed to following state

```
{  
   "id":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
   "metaJobExecutionId":"99dfac11-1caf-4470-9ad1-d533f6360bdd",
   "status":"LOADED",
   "fileDefinitions":[  
      {  
         "id":"88dfac11-1caf-4470-9ad1-d533f6360bdd",
         "uploadDefinitionId":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
         "jobExecutionId":"66dfac11-1caf-4470-9ad1-d533f6360bdd",
         "loaded":true,
         "name":"marc.mrc"
      }
   ]
}
```

### Add new FileDefinition to the existing UploadDefinition

##### Request for adding new file

```
curl -w '\n' -X POST -D -   \
   -H "Content-type: application/json"   \
   -d @fileDefinition.json \
   http://localhost:9130/data-import/upload/definition/file
```

Body of new fileDefinition should has uploadDefinitionId and file name.

```
{  
   "name":"marc3.mrc",
   "uploadDefinitionId":"67dfac11-1caf-4470-9ad1-d533f6360bdd"
}
```

##### Response with added FileDefinition

```
{  
   "id":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
   "metaJobExecutionId":"99dfac11-1caf-4470-9ad1-d533f6360bdd",
   "status":"IN_PROGRESS",
   "fileDefinitions":[  
      {  
         "id":"88dfac11-1caf-4470-9ad1-d533f6360bdd",
         "uploadDefinitionId":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
         "jobExecutionId":"66dfac11-1caf-4470-9ad1-d533f6360bdd",
         "loaded":true,
         "name":"marc.mrc"
      },
      {  
         "id":"77dfac11-1caf-4470-9ad1-d533f6360bdd",
         "uploadDefinitionId":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
         "jobExecutionId":"55dfac11-1caf-4470-9ad1-d533f6360bdd",
         "loaded":true,
         "name":"marc2.mrc"
      },
      {  
         "id":"33dfac11-1caf-4470-9ad1-d533f6360bdd",
         "uploadDefinitionId":"67dfac11-1caf-4470-9ad1-d533f6360bdd",
         "jobExecutionId":"22dfac11-1caf-4470-9ad1-d533f6360bdd",
         "loaded":false,
         "name":"marc3.mrc"
      }
   ]
}
```
