## 2019-02-14 v1.2.0-SNAPSHOT
* Created service for file chunking
* Implemented MARC file reader for local files
* Added CRUD for FileExtension entity
* Changed logging configuration to slf4j
* Optimized file upload functionality
* Used shared data-import-utils
* Renamed endpoints

   | METHOD |             URL                                                    | DESCRIPTION                     |
   |--------|--------------------------------------------------------------------|---------------------------------|
   | POST   | /data-import/uploadDefinitions                                     | Create Upload Definition        |
   | GET    | /data-import/uploadDefinitions                                     | Get list of Upload Definitions  |
   | GET    | /data-import/uploadDefinitions/{uploadDefinitionId}                | Get Upload Definition by id     |
   | PUT    | /data-import/uploadDefinitions/{uploadDefinitionId}                | Update Upload Definition        |
   | DELETE | /data-import/uploadDefinitions/{uploadDefinitionId}                | Delete Upload Definition        |
   | POST   | /data-import/uploadDefinitions/{uploadDefinitionId}/files/{fileId} | Upload file                     |
   | POST   | /data-import/uploadDefinitions/{uploadDefinitionId}/files          | Add file to Upload Definition   |
   | DELETE | /data-import/uploadDefinitions/{uploadDefinitionId}/files/{fileId} | Delete file                     |
   | POST   | /data-import/uploadDefinitions/{uploadDefinitionId}/processFiles   | Start file processing           |
   | GET    | /data-import/fileExtensions                                        | Get list of File Extensions     |
   | POST   | /data-import/fileExtensions                                        | Create File Extension           |
   | GET    | /data-import/fileExtensions/{id}                                   | Get File Extension by id        |
   | PUT    | /data-import/fileExtensions/{id}                                   | Update File Extension           |
   | DELETE | /data-import/fileExtensions/{id}                                   | Delete File Extension           |
   | GET    | /data-import/fileExtensions/restore/default                        | Restore default File Extensions |
   | GET    | /data-import/dataTypes                                             | Get list of DataTypes           |


## 2018-11-30 v1.0.0
 * Implemented functionality of file storing
 * Implemented functionality of file upload
