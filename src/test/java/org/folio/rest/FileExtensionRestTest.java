package org.folio.rest;

import static org.folio.support.TestUtil.DATA_TYPE_PATH;
import static org.folio.support.TestUtil.FILE_EXTENSION_PATH;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.DataType;
import org.folio.rest.jaxrs.model.FileExtension;
import org.folio.support.AbstractRestTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FileExtensionRestTest extends AbstractRestTest {

  private static final FileExtension FILE_EXTENSION_1 = new FileExtension()
    .withExtension(".marc").withDataTypes(Collections.singletonList(DataType.MARC)).withImportBlocked(false);
  private static final FileExtension FILE_EXTENSION_2 = new FileExtension()
    .withExtension(".edi").withDataTypes(Collections.singletonList(DataType.EDIFACT)).withImportBlocked(false);
  private static final FileExtension FILE_EXTENSION_3 = new FileExtension()
    .withExtension(".pdf").withDataTypes(new ArrayList<>()).withImportBlocked(true);
  private static final FileExtension FILE_EXTENSION_4 = new FileExtension()
    .withExtension(".marc").withDataTypes(new ArrayList<>()).withImportBlocked(true);
  private static final FileExtension FILE_EXTENSION_5 = new FileExtension()
    .withExtension("ma rc").withDataTypes(new ArrayList<>()).withImportBlocked(true);
  private static final FileExtension FILE_EXTENSION_6 = new FileExtension()
    .withExtension(".varc").withDataTypes(new ArrayList<>()).withImportBlocked(false);
  private static final FileExtension FILE_EXTENSION_7 = new FileExtension()
    .withExtension(".zarc").withDataTypes(new ArrayList<>()).withImportBlocked(false);

  @DisplayName("should return empty list when no file extensions exist")
  @Test
  void shouldReturnEmptyList_whenNoFileExtensionsExist() {
    getRequest(FILE_EXTENSION_PATH)
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .body("fileExtensions", empty());
  }

  @DisplayName("should return all file extensions when no query is specified")
  @Test
  void shouldReturnAllFileExtensions_whenNoQueryIsSpecified() {
    List<FileExtension> extensionsToPost = Arrays.asList(FILE_EXTENSION_1, FILE_EXTENSION_2, FILE_EXTENSION_3);
    for (FileExtension extension : extensionsToPost) {
      postRequest(FILE_EXTENSION_PATH, extension).statusCode(HttpStatus.SC_CREATED);
    }

    getRequest(FILE_EXTENSION_PATH)
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(extensionsToPost.size()));
  }

  @DisplayName("should return file extensions with importBlocked=false when queried")
  @Test
  void shouldReturnFileExtensionsWithBlockedImportFalse_whenQueried() {
    List<FileExtension> extensionsToPost = Arrays.asList(FILE_EXTENSION_1, FILE_EXTENSION_2, FILE_EXTENSION_3);
    for (FileExtension extension : extensionsToPost) {
      postRequest(FILE_EXTENSION_PATH, extension).statusCode(HttpStatus.SC_CREATED);
    }

    getRequest(FILE_EXTENSION_PATH + "?query=importBlocked=false")
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(2))
      .body("fileExtensions*.importBlocked", everyItem(is(false)));
  }

  @DisplayName("should return limited collection when limit query parameter is used")
  @Test
  void shouldReturnLimitedCollection_whenLimitQueryIsSpecified() {
    List<FileExtension> extensionsToPost = Arrays.asList(FILE_EXTENSION_1, FILE_EXTENSION_2, FILE_EXTENSION_3);
    for (FileExtension extension : extensionsToPost) {
      postRequest(FILE_EXTENSION_PATH, extension).statusCode(HttpStatus.SC_CREATED);
    }

    getRequest(FILE_EXTENSION_PATH + "?limit=2")
      .statusCode(HttpStatus.SC_OK)
      .body("fileExtensions.size()", is(2))
      .body("totalRecords", is(extensionsToPost.size()));
  }

  @DisplayName("should return 422 when extension is missing from request body")
  @Test
  void shouldReturn422_whenNoExtensionInBody() {
    postRequest(FILE_EXTENSION_PATH, new JsonObject().toString())
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @DisplayName("should return 422 when an invalid field is present in request body")
  @Test
  void shouldReturn422_whenInvalidFieldInBody() {
    JsonObject fileExtension = JsonObject.mapFrom(FILE_EXTENSION_1).put("invalidField", "value");

    postRequest(FILE_EXTENSION_PATH, fileExtension.encode())
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @DisplayName("should create file extension when valid body is provided")
  @Test
  void shouldCreateFileExtension_whenValidBodyProvided() {
    postRequest(FILE_EXTENSION_PATH, FILE_EXTENSION_2)
      .statusCode(HttpStatus.SC_CREATED)
      .body("extension", is(FILE_EXTENSION_2.getExtension()))
      .body("dataTypes.size()", is(FILE_EXTENSION_2.getDataTypes().size()))
      .body("importBlocked", is(FILE_EXTENSION_2.getImportBlocked()));
  }

  @DisplayName("should return 422 on PUT when extension is missing from body")
  @Test
  void shouldReturn422OnPut_whenNoExtensionInBody() {
    putRequest(FILE_EXTENSION_PATH + "/" + UUID.randomUUID(), new JsonObject().toString())
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @DisplayName("should return 422 on PUT when invalid field is present")
  @Test
  void shouldReturn422OnPut_whenInvalidFieldInBody() {
    JsonObject invalidFileExtension = JsonObject.mapFrom(FILE_EXTENSION_1).put("invalidField", "value");

    putRequest(FILE_EXTENSION_PATH + "/" + UUID.randomUUID(), invalidFileExtension.encode())
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @DisplayName("should return 404 on PUT when file extension does not exist")
  @Test
  void shouldReturn404OnPut_whenFileExtensionDoesNotExist() {
    putRequest(FILE_EXTENSION_PATH + "/" + UUID.randomUUID(), FILE_EXTENSION_1)
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @DisplayName("should update existing file extension on PUT")
  @Test
  void shouldUpdateFileExtension_whenItExists() {
    FileExtension fileExtension = postRequest(FILE_EXTENSION_PATH, FILE_EXTENSION_1)
      .statusCode(HttpStatus.SC_CREATED)
      .extract().body().as(FileExtension.class);

    fileExtension.setImportBlocked(true);
    fileExtension.setMetadata(null);

    putRequest(FILE_EXTENSION_PATH + "/" + fileExtension.getId(), fileExtension)
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(fileExtension.getId()))
      .body("extension", is(fileExtension.getExtension()))
      .body("dataTypes.size()", is(fileExtension.getDataTypes().size()))
      .body("dataTypes.get(0)", is(fileExtension.getDataTypes().getFirst().value()))
      .body("importBlocked", is(true));
  }

  @DisplayName("should return 404 on GET by ID when file extension does not exist")
  @Test
  void shouldReturn404OnGetById_whenFileExtensionDoesNotExist() {
    getRequest(FILE_EXTENSION_PATH + "/" + UUID.randomUUID())
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @DisplayName("should return file extension by ID when it exists")
  @Test
  void shouldReturnFileExtensionById_whenItExists() {
    FileExtension fileExtension = postRequest(FILE_EXTENSION_PATH, FILE_EXTENSION_3)
      .statusCode(HttpStatus.SC_CREATED)
      .extract().body().as(FileExtension.class);

    getRequest(FILE_EXTENSION_PATH + "/" + fileExtension.getId())
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(fileExtension.getId()))
      .body("extension", is(fileExtension.getExtension()))
      .body("dataTypes", is(fileExtension.getDataTypes()))
      .body("importBlocked", is(fileExtension.getImportBlocked()));
  }

  @DisplayName("should return 404 on DELETE when file extension does not exist")
  @Test
  void shouldReturn404OnDelete_whenFileExtensionDoesNotExist() {
    deleteRequest(FILE_EXTENSION_PATH + "/" + UUID.randomUUID())
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @DisplayName("should delete existing file extension successfully")
  @Test
  void shouldDeleteFileExtension_whenItExists() {
    FileExtension fileExtension = postRequest(FILE_EXTENSION_PATH, FILE_EXTENSION_1)
      .statusCode(HttpStatus.SC_CREATED)
      .extract().body().as(FileExtension.class);

    deleteRequest(FILE_EXTENSION_PATH + "/" + fileExtension.getId())
      .statusCode(HttpStatus.SC_NO_CONTENT);
  }

  @DisplayName("should return 422 when updating extension name to a duplicate")
  @Test
  void shouldReturn422_whenUpdatingExtensionNameToDuplicate() {
    postRequest(FILE_EXTENSION_PATH, FILE_EXTENSION_6).statusCode(HttpStatus.SC_CREATED);

    FileExtension fileExtension = postRequest(FILE_EXTENSION_PATH, FILE_EXTENSION_7)
      .statusCode(HttpStatus.SC_CREATED)
      .extract().body().as(FileExtension.class);

    putRequest(FILE_EXTENSION_PATH + "/" + fileExtension.getId(),
      fileExtension.withExtension(FILE_EXTENSION_6.getExtension()).withMetadata(null))
      .log().all()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
      .body("errors[0].message", is("File extension .varc already exists"));
  }

  @DisplayName("should return 422 when saving a duplicate extension")
  @Test
  void shouldReturn422_whenSavingDuplicateExtension() {
    postRequest(FILE_EXTENSION_PATH, FILE_EXTENSION_1)
      .statusCode(HttpStatus.SC_CREATED)
      .body("extension", is(FILE_EXTENSION_1.getExtension()))
      .body("dataTypes.size()", is(FILE_EXTENSION_1.getDataTypes().size()))
      .body("importBlocked", is(FILE_EXTENSION_1.getImportBlocked()));

    postRequest(FILE_EXTENSION_PATH, FILE_EXTENSION_4)
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
      .body("errors[0].message", is("File extension .marc already exists"));

    postRequest(FILE_EXTENSION_PATH, FILE_EXTENSION_4.withExtension(" " + FILE_EXTENSION_4.getExtension() + " "))
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
      .body("errors[0].message", is("File extension  .marc  is not a valid format"));
  }

  @DisplayName("should return all data types when GET /dataTypes is called")
  @Test
  void shouldReturnAllDataTypes_whenGetDataTypesIsCalled() {
    String[] dataTypesNames = Arrays.stream(DataType.values()).map(Enum::toString).toArray(String[]::new);

    getRequest(DATA_TYPE_PATH)
      .log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(DataType.values().length))
      .body("dataTypes", hasItems(dataTypesNames));
  }

  @DisplayName("should return 422 when saving extension with invalid format")
  @Test
  void shouldReturn422_whenSavingInvalidExtension() {
    postRequest(FILE_EXTENSION_PATH, FILE_EXTENSION_5)
      .log().all()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
      .body("errors[0].message", is("File extension ma rc is not a valid format"));
  }
}
