## 2022-11-03 v2.6.2
* [MODDATAIMP-730](https://issues.folio.org/browse/MODDATAIMP-730) Spring 5.3, kafkaclients 3.2.3, folio-di-support 1.7.0

## 2022-10-31 v2.6.1
* [MODDATAIMP-728](https://issues.folio.org/browse/MODDATAIMP-728) Set vertx version to 4.3.1

## 2022-10-24 v2.6.0
* [MODDATAIMP-721](https://issues.folio.org/browse/MODDATAIMP-721) Upgrade RMB to v35.0.1
* [MODDATAIMP-709](https://issues.folio.org/browse/MODDATAIMP-709) Supports users interface 15.0, 16.0
* [MODDATAIMP-714](https://issues.folio.org/browse/MODDATAIMP-714) Assign each authority record to an Authority Source file list

## 2022-07-05 v2.5.0
* [MODDATAIMP-472](https://issues.folio.org/browse/MODDATAIMP-472) EDIFACT files with txt file extensions do not import
* [MODDATAIMP-646](https://issues.folio.org/browse/MODDATAIMP-646) Logs show incorrectly formatted request id
* [MODDATAIMP-696](https://issues.folio.org/browse/MODDATAIMP-696) Upgrade RMB to v34.1.0

## 2022-03-03 v2.4.0
* Update RMB to v33.2.6
* [MODDATAIMP-468](https://issues.folio.org/browse/MODDATAIMP-468) Update source-manager-job-executions interface 2.3 to 3.0
* [MODDATAIMP-494](https://issues.folio.org/browse/MODDATAIMP-494) Improve logging

## 2021-12-15 v2.2.1
* [MODDATAIMP-598](https://issues.folio.org/browse/MODDATAIMP-598) Log4j (CVE-2021-44228) vulnerability correction

## 2021-10-06 v2.2.0
* [MODSOURMAN-550](https://issues.folio.org/browse/MODSOURMAN-550) Reduce BE response payload for DI Landing Page to increase performance
* [MODDATAIMP-480](https://issues.folio.org/browse/MODDATAIMP-480) Suppress harmless errors from Data Import logs
* [MODDATAIMP-548](https://issues.folio.org/browse/MODDATAIMP-548) Provide system properties to set records chunk size for each record type and marc format
* [MODDATAIMP-511](https://issues.folio.org/browse/MODDATAIMP-511) Upgrade to RAML Module Builder 33.x
* [MODDATAIMP-491](https://issues.folio.org/browse/MODDATAIMP-491) Improve logging to be able to trace the path of each record and file_chunks

## 2021-10-05 v2.1.3
* [MODDATAIMP-465](https://issues.folio.org/browse/MODDATAIMP-465) Fix memory leaks - close Vertx Kafka producers

## 2021-08-02 v2.1.2
* [MODDATAIMP-459](https://issues.folio.org/browse/MODDATAIMP-459) EDIFACT files with CAPS file extensions do not import
* [MODDATAIMP-514](https://issues.folio.org/browse/MODDATAIMP-514) Add support for max.request.size configuration for Kafka messages

## 2021-06-25 v2.1.1
* [MODDATAIMP-464](https://issues.folio.org/browse/MODDATAIMP-464) Change dataType to have have common type for MARC related subtypes
* Update data-import-processing-core dependency to v3.1.2 

## 2021-06-17 v2.1.0
* [MODDATAIMP-433](https://issues.folio.org/browse/MODDATAIMP-433) Store MARC Authority Records
* [MODDATAIMP-451](https://issues.folio.org/browse/MODDATAIMP-451) Update interface version

## 2021-04-23 v2.0.2
* [MODDATAIMP-413](https://issues.folio.org/browse/MODDATAIMP-413) When a file is uploaded for data import, the file extension check should be case-insensitive

## 2021-04-09 v2.0.1
* [MODDATAIMP-388](https://issues.folio.org/browse/MODDATAIMP-388) Import job is not completed on file parsing error
* [MODDATAIMP-400](https://issues.folio.org/browse/MODDATAIMP-400) Resolve data-import catching error issues

## 2021-03-18 v2.0.0
* [MODDATAIMP-315](https://issues.folio.org/browse/MODDATAIMP-315) Use Kafka for data-import file processing
* [MODDATAIMP-352](https://issues.folio.org/browse/MODDATAIMP-352) Implement source reader for EDIFACT files.
* [MODDATAIMP-358](https://issues.folio.org/browse/MODDATAIMP-358) Upgrade to RAML Module Builder 32.x.
* [MODDATAIMP-348](https://issues.folio.org/browse/MODDATAIMP-348) Add personal data disclosure form.

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
