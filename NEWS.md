## xxxx-xx-xx v1.12.0-SNAPSHOT

## 2020-11-06 v1.11.1
* [MODDATAIMP-316](https://issues.folio.org/browse/MODDATAIMP-316) Disable CQL2PgJSON & CQLWrapper extra logging in mod-data-import
* [MODDATAIMP-342](https://issues.folio.org/browse/MODDATAIMP-342) Upgrade to RMB v31.1.5

## 2020-10-14 v1.11.0
* Add batch-MARC-import script, `scripts/load-marc-data-into-folio.sh`
* [MODDATAIMP-324](https://issues.folio.org/browse/MODDATAIMP-324) Update all Data-Import modules to the new RMB version
* [MODDATAIMP-338](https://issues.folio.org/browse/MODDATAIMP-338) Data-import job prevents all users from uploading a file and initiating another data-import job
* [MODDATAIMP-325](https://issues.folio.org/browse/MODDATAIMP-325) Remove delimited reference values from file extensions data settings

## 2020-06-12 v1.10.0
* [MODDATAIMP-300](https://issues.folio.org/browse/MODDATAIMP-300) Updated marc4j version to 2.9.1
* Updated reference to raml-storage
* [MODDATAIMP-301](https://issues.folio.org/browse/MODDATAIMP-301) Upgrade to RMB 30.0.2
* [MODDATAIMP-304](https://issues.folio.org/browse/MODDATAIMP-304) Lack of details in case of an error during file processing

## 2020-03-23 v1.9.1
* [MODDATAIMP-296](https://issues.folio.org/browse/MODDATAIMP-296) Added migration script to support RMB version update

## 2020-03-13 v1.9.0
* Updated RMB version to 29.1.5
* Added defaultMapping query param

## 2019-12-04 v1.8.0
* Applied new JVM features to manage container memory
* Fixed security vulnerabilities

## 2019-11-04 v1.7.0
* Fixed encoding issues
* Added order of the record in importing file 

## 2019-09-25 v1.6.1
* Added blocking coordination to process files in sequential manner

## 2019-09-10 v1.6.0
* Added total records counter for ChunkProcessing 
* Updated schemas for support new RawRecords
* Filled in "fromModuleVersion" value for each "tables" section in schema.json

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
