## 2019-07-24 v1.6.0
* Added total records counter for ChunkProcessing 
* Updated schemas for support new RawRecords

## 2019-07-24 v1.5.0
* Updated README with information about test mode of the module.
* Updated documentation on file processing api.

## 2019-06-12 v1.4.0
* Added support for incoming xml files containing MARC records.
* Added contentType field to the RawRecordsDto that describes type of records (MARC, EDIFACT etc) and format of record 
representation(JSON, XML, RAW).

## 2019-05-17 v1.3.1
* Fixed JobExecution status update error

## 2019-05-12 v1.3.0
* Added Spring DI support
* Added dependency on users interface
* Added support for incoming json files containing MARC records

## 2019-03-25 v1.2.1
* Fixed creating FileProcessor instance used in ProxyGen service
* Fixed updating JobProfile for jobs during the file processing

## 2019-03-20 v1.2.0
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
   | POST   | /data-import/fileExtensions/restore/default                        | Restore default File Extensions |
   | GET    | /data-import/dataTypes                                             | Get list of DataTypes           |


## 2018-11-30 v1.0.0
 * Implemented functionality of file storing
 * Implemented functionality of file upload
