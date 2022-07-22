# File Upload API

## Introduction

This API is responsible for uploading files to storage. 
**mod-data-import** module use uploaded files for parsing and importing files to storage and another FOLIO modules usage.
File Upload API provides an ability to upload several files and store state of uploaded files. 
A user can delete or add another file from uploading session.

## Entities

### UploadDefinition

Upload Definition entity describes an upload process and contains a list of file definitions.
Each file upload session process starts from creating UploadDefinition.


| Field | Description |
| ------ | ------ |
| id | UUID for an UploadDefinition entity |
| metaJobExecutionId | UUID of JobExecution process for all linked files. Specific for data-import application. |
| status | Show the current state of the upload process. Can be one of: "NEW", "IN_PROGRESS", "LOADED", "COMPLETED" |
| createDate | Date and time when the upload definition was created |
| fileDefinitions | Array of the FileDefinition entities. Each FileDefinition describes particular state of the file |
| metadata | Standard field with metainformation about entity |

### FileDefinition

File Definition entity describes an upload process of particular file and store this process's state.

| Field | Description |
| ------ | ------ |
| id | UUID for an FileDefinition entity |
| sourcePath | The path to the file location in the storage. Readonly |
| name | Name of the file with its extension. This  field is required. |
| status | Show the current state of the upload file. Can be one of: "NEW", "UPLOADING", "UPLOADED", "ERROR" |
| jobExecutionId | UUID of the file's JobExecution process. Specific for data-import application. |
| uploadDefinitionId | UUID of UploadDefinition related to this file |
| createDate | Date and time when the file definition was created |
| uploadedDate | Date and time when the file was uploaded |
| size | Size of the file in Kbyte |
| uiKey | Unique key for the file definition on user interface before entity saved |

## File Upload API

For all necessary file upload lifecycle operations mod-data-import provides following endpoints: 

| Method | URL | ContentType |Description |
| ------ |------ | ------ |------ |
| **POST** | /data-import/uploadDefinitions | application/json | Create new UploadDefinition |
| **GET** | /data-import/uploadDefinitions | | Get list of UploadDefinitions by query |
| **GET** | /data-import/uploadDefinitions/**{uploadDefinitionId}** | | Get single definition by UUID |
| **PUT** | /data-import/uploadDefinitions/**{uploadDefinitionId}** | application/json | Update definition by UUID |
| **DELETE** | /data-import/uploadDefinitions/**{uploadDefinitionId}** | | Delete UploadDefinition by UUID |
| **POST** | /data-import/uploadDefinitions/**{uploadDefinitionId}**/files | application/json | Add new FileDefinition to UploadDefinition |
| **DELETE** | /data-import/uploadDefinitions/**{uploadDefinitionId}**/files/**{fileId}** | | Delete FileDefinition from UploadDefinition by UUID |
| **POST** | /data-import/uploadDefinitions/**{uploadDefinitionId}**/files/**{fileId}** | application/octet-stream | Upload file data to backend storage |

## File Upload Workflow

### Create UploadDefinition

Each file uploading process starts from creating new UploadDefinition. 
Send POST request with list of file names for creating new UploadDefinition.

```
curl -w '\n' -X POST -D -   \
   -H "Content-type: application/json"   \
   -H "x-okapi-tenant: diku"  \
   -H "x-okapi-token: eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkaWt1X2FkbWluIiwidXNlcl9pZCI6IjVlMWZkMGViLTY0YmYtNWQ0YS1iNmVkLWJiMDg4OTAwOGJiYyIsImNhY2hlX2tleSI6IjVmY2VkODQzLTZlMGItNDgxOC1iZDcwLWU3N2M2ZjQyZWQyNyIsImlhdCI6MTU1MzE4MjUxMSwidGVuYW50IjoiZGlrdSJ9.7te4Y5PYRUmLZHuq1HjkF-HMLwdzTJ2oba613whOlgc" \
   -d @uploadDefinitionRequest.json \
   http://localhost:9130/data-import/uploadDefinitions
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
  "id" : "71a43ec9-d923-4c44-8405-979af23b7cc9",
  "metaJobExecutionId" : "4044bf4d-fb53-4b01-81e9-fafff1024dde",
  "status" : "NEW",
  "createDate" : "2019-03-21T15:36:49.583+0000",
  "fileDefinitions" : [ {
    "id" : "776c7413-7ad9-467b-a686-775a434d2505",
    "name" : "marc.mrc",
    "status" : "NEW",
    "jobExecutionId" : "81f69f1d-e429-490c-a4c4-4eda2eb31791",
    "uploadDefinitionId" : "71a43ec9-d923-4c44-8405-979af23b7cc9",
    "createDate" : "2019-03-21T15:36:49.583+0000"
  }, {
    "id" : "8076b8ea-b04f-46b7-b793-0347d994c2be",
    "name" : "marc2.mrc",
    "status" : "NEW",
    "jobExecutionId" : "c242e303-2649-4131-af60-d272130ff466",
    "uploadDefinitionId" : "71a43ec9-d923-4c44-8405-979af23b7cc9",
    "createDate" : "2019-03-21T15:36:49.583+0000"
  } ],
  "metadata" : {
    "createdDate" : "2019-03-21T15:36:49.582+0000",
    "createdByUserId" : "5e1fd0eb-64bf-5d4a-b6ed-bb0889008bbc",
    "updatedDate" : "2019-03-21T15:36:49.582+0000",
    "updatedByUserId" : "5e1fd0eb-64bf-5d4a-b6ed-bb0889008bbc"
  }
}

```

### Upload file body to the backend
After UploadDefinition successfully created, backend ready to file uploading. 
For each file send a request with file's data. Content-type: application/octet-stream and body of request should be a file.

```
curl -w '\n' -X POST -D -   \
   -H "Content-type: application/octet-stream"   \
   -H "x-okapi-tenant: diku"  \
   -H "x-okapi-token: eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkaWt1X2FkbWluIiwidXNlcl9pZCI6IjVlMWZkMGViLTY0YmYtNWQ0YS1iNmVkLWJiMDg4OTAwOGJiYyIsImNhY2hlX2tleSI6IjVmY2VkODQzLTZlMGItNDgxOC1iZDcwLWU3N2M2ZjQyZWQyNyIsImlhdCI6MTU1MzE4MjUxMSwidGVuYW50IjoiZGlrdSJ9.7te4Y5PYRUmLZHuq1HjkF-HMLwdzTJ2oba613whOlgc" \
   -d @marc.mrc \
   http://localhost:9130/data-import/uploadDefinitions/71a43ec9-d923-4c44-8405-979af23b7cc9/files/776c7413-7ad9-467b-a686-775a434d2505   
```

##### Response with changed UploadDefinition

If the first file is loaded, UploadDefinition change status to "IN_PROGRESS" and "loaded" field of particular FileDefinition will be changed on "true" value.

```
{
  "id" : "71a43ec9-d923-4c44-8405-979af23b7cc9",
  "metaJobExecutionId" : "4044bf4d-fb53-4b01-81e9-fafff1024dde",
  "status" : "IN_PROGRESS",
  "createDate" : "2019-03-21T15:36:49.583+0000",
  "fileDefinitions" : [ {
    "id" : "8076b8ea-b04f-46b7-b793-0347d994c2be",
    "name" : "marc2.mrc",
    "status" : "NEW",
    "jobExecutionId" : "c242e303-2649-4131-af60-d272130ff466",
    "uploadDefinitionId" : "71a43ec9-d923-4c44-8405-979af23b7cc9",
    "createDate" : "2019-03-21T15:36:49.583+0000"
  }, {
    "id" : "776c7413-7ad9-467b-a686-775a434d2505",
    "sourcePath" : "./storage/upload/71a43ec9-d923-4c44-8405-979af23b7cc9/776c7413-7ad9-467b-a686-775a434d2505/marc.mrc",
    "name" : "marc.mrc",
    "status" : "UPLOADED",
    "jobExecutionId" : "81f69f1d-e429-490c-a4c4-4eda2eb31791",
    "uploadDefinitionId" : "71a43ec9-d923-4c44-8405-979af23b7cc9",
    "createDate" : "2019-03-21T15:36:49.583+0000",
    "uploadedDate" : "2019-03-21T15:43:44.333+0000"
  } ],
  "metadata" : {
    "createdDate" : "2019-03-21T15:36:49.582+0000",
    "createdByUserId" : "5e1fd0eb-64bf-5d4a-b6ed-bb0889008bbc",
    "updatedDate" : "2019-03-21T15:36:49.582+0000",
    "updatedByUserId" : "5e1fd0eb-64bf-5d4a-b6ed-bb0889008bbc"
  }
}
```

##### Upload last file of the UploadDefinition

```
curl -w '\n' -X POST -D -   \
   -H "Content-type: application/octet-stream"   \
   -H "x-okapi-tenant: diku"  \
   -H "x-okapi-token: eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkaWt1X2FkbWluIiwidXNlcl9pZCI6IjVlMWZkMGViLTY0YmYtNWQ0YS1iNmVkLWJiMDg4OTAwOGJiYyIsImNhY2hlX2tleSI6IjVmY2VkODQzLTZlMGItNDgxOC1iZDcwLWU3N2M2ZjQyZWQyNyIsImlhdCI6MTU1MzE4MjUxMSwidGVuYW50IjoiZGlrdSJ9.7te4Y5PYRUmLZHuq1HjkF-HMLwdzTJ2oba613whOlgc" \
   -d @marc2.mrc \
   http://localhost:9130/data-import/uploadDefinitions/71a43ec9-d923-4c44-8405-979af23b7cc9/files/8076b8ea-b04f-46b7-b793-0347d994c2be
```

##### Response with changed UploadDefinition

As far as last file will be loaded, UploadDefinition change status to "LOADED"
```
{
  "id" : "71a43ec9-d923-4c44-8405-979af23b7cc9",
  "metaJobExecutionId" : "4044bf4d-fb53-4b01-81e9-fafff1024dde",
  "status" : "LOADED",
  "createDate" : "2019-03-21T15:36:49.583+0000",
  "fileDefinitions" : [ {
    "id" : "776c7413-7ad9-467b-a686-775a434d2505",
    "sourcePath" : "./storage/upload/71a43ec9-d923-4c44-8405-979af23b7cc9/776c7413-7ad9-467b-a686-775a434d2505/marc.mrc",
    "name" : "marc.mrc",
    "status" : "UPLOADED",
    "jobExecutionId" : "81f69f1d-e429-490c-a4c4-4eda2eb31791",
    "uploadDefinitionId" : "71a43ec9-d923-4c44-8405-979af23b7cc9",
    "createDate" : "2019-03-21T15:36:49.583+0000",
    "uploadedDate" : "2019-03-21T15:43:44.333+0000"
  }, {
    "id" : "8076b8ea-b04f-46b7-b793-0347d994c2be",
    "sourcePath" : "./storage/upload/71a43ec9-d923-4c44-8405-979af23b7cc9/8076b8ea-b04f-46b7-b793-0347d994c2be/marc2.mrc",
    "name" : "marc2.mrc",
    "status" : "UPLOADED",
    "jobExecutionId" : "c242e303-2649-4131-af60-d272130ff466",
    "uploadDefinitionId" : "71a43ec9-d923-4c44-8405-979af23b7cc9",
    "createDate" : "2019-03-21T15:36:49.583+0000",
    "uploadedDate" : "2019-03-21T15:49:26.538+0000"
  } ],
  "metadata" : {
    "createdDate" : "2019-03-21T15:36:49.582+0000",
    "createdByUserId" : "5e1fd0eb-64bf-5d4a-b6ed-bb0889008bbc",
    "updatedDate" : "2019-03-21T15:36:49.582+0000",
    "updatedByUserId" : "5e1fd0eb-64bf-5d4a-b6ed-bb0889008bbc"
  }
}

```

### Delete FileDefinition

##### Request for deleting second file

```
curl -X DELETE -D - -w '\n' \
  -H "x-okapi-tenant: diku"  \
  -H "x-okapi-token: eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkaWt1X2FkbWluIiwidXNlcl9pZCI6IjVlMWZkMGViLTY0YmYtNWQ0YS1iNmVkLWJiMDg4OTAwOGJiYyIsImNhY2hlX2tleSI6IjVmY2VkODQzLTZlMGItNDgxOC1iZDcwLWU3N2M2ZjQyZWQyNyIsImlhdCI6MTU1MzE4MjUxMSwidGVuYW50IjoiZGlrdSJ9.7te4Y5PYRUmLZHuq1HjkF-HMLwdzTJ2oba613whOlgc" \
  http://localhost:9130/data-import/uploadDefinitions/71a43ec9-d923-4c44-8405-979af23b7cc9/files/8076b8ea-b04f-46b7-b793-0347d994c2be        
```

##### Response for deleting second file will be 204

UploadDefinition entity will be changed to following state
```
{
  "id" : "71a43ec9-d923-4c44-8405-979af23b7cc9",
  "metaJobExecutionId" : "4044bf4d-fb53-4b01-81e9-fafff1024dde",
  "status" : "LOADED",
  "createDate" : "2019-03-21T15:36:49.583+0000",
  "fileDefinitions" : [ {
    "id" : "776c7413-7ad9-467b-a686-775a434d2505",
    "sourcePath" : "./storage/upload/71a43ec9-d923-4c44-8405-979af23b7cc9/776c7413-7ad9-467b-a686-775a434d2505/marc.mrc",
    "name" : "marc.mrc",
    "status" : "UPLOADED",
    "jobExecutionId" : "81f69f1d-e429-490c-a4c4-4eda2eb31791",
    "uploadDefinitionId" : "71a43ec9-d923-4c44-8405-979af23b7cc9",
    "createDate" : "2019-03-21T15:36:49.583+0000",
    "uploadedDate" : "2019-03-21T15:43:44.333+0000"
  } ],
  "metadata" : {
    "createdDate" : "2019-03-21T15:36:49.582+0000",
    "createdByUserId" : "5e1fd0eb-64bf-5d4a-b6ed-bb0889008bbc",
    "updatedDate" : "2019-03-21T15:36:49.582+0000",
    "updatedByUserId" : "5e1fd0eb-64bf-5d4a-b6ed-bb0889008bbc"
  }
}
```

### Add new FileDefinition to the existing UploadDefinition

##### Request for adding new file

```
curl -w '\n' -X POST -D -   \
   -H "Content-type: application/json"   \
   -H "x-okapi-tenant: diku"  \
   -H "x-okapi-token: eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkaWt1X2FkbWluIiwidXNlcl9pZCI6IjVlMWZkMGViLTY0YmYtNWQ0YS1iNmVkLWJiMDg4OTAwOGJiYyIsImNhY2hlX2tleSI6IjVmY2VkODQzLTZlMGItNDgxOC1iZDcwLWU3N2M2ZjQyZWQyNyIsImlhdCI6MTU1MzE4MjUxMSwidGVuYW50IjoiZGlrdSJ9.7te4Y5PYRUmLZHuq1HjkF-HMLwdzTJ2oba613whOlgc" \
   -d @fileDefinition.json \
   http://localhost:9130/data-import/uploadDefinitions/71a43ec9-d923-4c44-8405-979af23b7cc9/files   
```

Body of new fileDefinition should has uploadDefinitionId and file name.

```
{  
   "name":"marc3.mrc",
   "uploadDefinitionId":"71a43ec9-d923-4c44-8405-979af23b7cc9"
}
```

##### Response with added FileDefinition

```
{
  "id" : "71a43ec9-d923-4c44-8405-979af23b7cc9",
  "metaJobExecutionId" : "4044bf4d-fb53-4b01-81e9-fafff1024dde",
  "status" : "LOADED",
  "createDate" : "2019-03-21T15:36:49.583+0000",
  "fileDefinitions" : [ {
    "id" : "776c7413-7ad9-467b-a686-775a434d2505",
    "sourcePath" : "./storage/upload/71a43ec9-d923-4c44-8405-979af23b7cc9/776c7413-7ad9-467b-a686-775a434d2505/marc.mrc",
    "name" : "marc.mrc",
    "status" : "UPLOADED",
    "jobExecutionId" : "81f69f1d-e429-490c-a4c4-4eda2eb31791",
    "uploadDefinitionId" : "71a43ec9-d923-4c44-8405-979af23b7cc9",
    "createDate" : "2019-03-21T15:36:49.583+0000",
    "uploadedDate" : "2019-03-21T15:43:44.333+0000"
  }, {
    "id" : "3e98acdf-fae3-4e4f-a5c6-ce5c7aa11fa7",
    "name" : "marc3.mrc",
    "status" : "NEW",
    "uploadDefinitionId" : "71a43ec9-d923-4c44-8405-979af23b7cc9",
    "createDate" : "2019-03-21T16:30:47.652+0000"
  } ],
  "metadata" : {
    "createdDate" : "2019-03-21T15:36:49.582+0000",
    "createdByUserId" : "5e1fd0eb-64bf-5d4a-b6ed-bb0889008bbc",
    "updatedDate" : "2019-03-21T15:36:49.582+0000",
    "updatedByUserId" : "5e1fd0eb-64bf-5d4a-b6ed-bb0889008bbc"
  }
}
```

## Necessary steps for file upload

### Authentication

```
curl -w '\n' -X POST -D -   \
   -H "Content-type: application/json"   \
   -H "x-okapi-tenant: diku"  \
   -d '{"username":"diku_admin","password":"admin"}'  \
   http://localhost:9130/bl-users/login
```

##### Response with necessary x-okapi-token header
```
x-okapi-token: eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkaWt1X2FkbWluIiwidXNlcl9pZCI6IjVlMWZkMGViLTY0YmYtNWQ0YS1iNmVkLWJiMDg4OTAwOGJiYyIsImNhY2hlX2tleSI6IjFmZGNiMzVhLTkwNjEtNGY1Yy1iN2JjLTA1YjI3NzJkZDI1ZSIsImlhdCI6MTU1MzE4NzEzOSwidGVuYW50IjoiZGlrdSJ9.8g9ZGkFkRaM3FbklzlMI1oL4iOH4pF6jvMSRXG5WiQk
```

### Create UploadDefinition
```
curl -w '\n' -X POST -D -   \
   -H "Content-type: application/json"   \
   -H "x-okapi-tenant: diku"  \
   -H "x-okapi-token: eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkaWt1X2FkbWluIiwidXNlcl9pZCI6IjVlMWZkMGViLTY0YmYtNWQ0YS1iNmVkLWJiMDg4OTAwOGJiYyIsImNhY2hlX2tleSI6IjVmY2VkODQzLTZlMGItNDgxOC1iZDcwLWU3N2M2ZjQyZWQyNyIsImlhdCI6MTU1MzE4MjUxMSwidGVuYW50IjoiZGlrdSJ9.7te4Y5PYRUmLZHuq1HjkF-HMLwdzTJ2oba613whOlgc" \
   -d @uploadDefinitionRequest.json \
   http://localhost:9130/data-import/uploadDefinitions
```

##### uploadDefinitionRequest.json

```
{  
   "fileDefinitions":[  
      {  
         "name":"marc.mrc"
      }      
   ]
}
```

### Upload file to the backend

```
curl -w '\n' -X POST -D -   \
   -H "Content-type: application/octet-stream"   \
   -H "x-okapi-tenant: diku"  \
   -H "x-okapi-token: eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkaWt1X2FkbWluIiwidXNlcl9pZCI6IjVlMWZkMGViLTY0YmYtNWQ0YS1iNmVkLWJiMDg4OTAwOGJiYyIsImNhY2hlX2tleSI6IjVmY2VkODQzLTZlMGItNDgxOC1iZDcwLWU3N2M2ZjQyZWQyNyIsImlhdCI6MTU1MzE4MjUxMSwidGVuYW50IjoiZGlrdSJ9.7te4Y5PYRUmLZHuq1HjkF-HMLwdzTJ2oba613whOlgc" \
   -d @marc.mrc \
   http://localhost:9130/data-import/uploadDefinitions/71a43ec9-d923-4c44-8405-979af23b7cc9/files/776c7413-7ad9-467b-a686-775a434d2505   
```
