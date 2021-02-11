package org.folio.service.fileextension;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.dao.FileExtensionDao;
import org.folio.dao.FileExtensionDaoImpl;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.rest.jaxrs.model.DataType;
import org.folio.rest.jaxrs.model.DataTypeCollection;
import org.folio.rest.jaxrs.model.FileExtension;
import org.folio.rest.jaxrs.model.FileExtensionCollection;
import org.folio.rest.jaxrs.model.UserInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class FileExtensionServiceImpl implements FileExtensionService {
  private static final Logger LOGGER = LogManager.getLogger();
  private static final String GET_USER_URL = "/users?query=id==";

  @Autowired
  private FileExtensionDao fileExtensionDao;

  public FileExtensionServiceImpl() {
  }

  /**
   * This constructor is used till {@link org.folio.service.processing.ParallelFileChunkingProcessor}
   * will be rewritten with DI support.
   *
   * @param vertx
   */
  public FileExtensionServiceImpl(Vertx vertx) {
    this.fileExtensionDao = new FileExtensionDaoImpl(vertx);
  }

  @Override
  public Future<FileExtensionCollection> getFileExtensions(String query, int offset, int limit, String tenantId) {
    return fileExtensionDao.getFileExtensions(query, offset, limit, tenantId);
  }

  @Override
  public Future<Optional<FileExtension>> getFileExtensionById(String id, String tenantId) {
    return fileExtensionDao.getFileExtensionById(id, tenantId);
  }

  @Override
  public Future<Optional<FileExtension>> getFileExtensionByExtenstion(String extension, String tenantId) {
    return fileExtensionDao.getFileExtensionByExtenstion(extension, tenantId);
  }

  @Override
  public Future<FileExtension> addFileExtension(FileExtension fileExtension, OkapiConnectionParams params) {
    fileExtension.setId(UUID.randomUUID().toString());
    fileExtension.setDataTypes(sortDataTypes(fileExtension.getDataTypes()));
    String userId = fileExtension.getMetadata().getUpdatedByUserId();
    return lookupUser(userId, params).compose(userInfo -> {
      fileExtension.setUserInfo(userInfo);
      return fileExtensionDao.addFileExtension(fileExtension, params.getTenantId()).map(fileExtension);
    });
  }

  @Override
  public Future<FileExtension> updateFileExtension(FileExtension fileExtension, OkapiConnectionParams params) {
    String userId = fileExtension.getMetadata().getUpdatedByUserId();
    return getFileExtensionById(fileExtension.getId(), params.getTenantId())
      .compose(optionalFileExtension -> optionalFileExtension.map(fileExt -> lookupUser(userId, params).compose(userInfo -> {
          fileExtension.setUserInfo(userInfo);
          return fileExtensionDao.updateFileExtension(fileExtension.withDataTypes(sortDataTypes(fileExtension.getDataTypes())), params.getTenantId());
        })
      ).orElse(Future.failedFuture(new NotFoundException(String.format("FileExtension with id '%s' was not found", fileExtension.getId())))));
  }

  @Override
  public Future<Boolean> deleteFileExtension(String id, String tenantId) {
    return fileExtensionDao.deleteFileExtension(id, tenantId);
  }

  @Override
  public Future<FileExtensionCollection> restoreFileExtensions(String tenantId) {
    return fileExtensionDao.restoreFileExtensions(tenantId);
  }

  @Override
  public Future<RowSet<Row>> copyExtensionsFromDefault(String tenantId) {
    return fileExtensionDao.copyExtensionsFromDefault(tenantId);
  }

  private List<DataType> sortDataTypes(List<DataType> list) {
    if (list == null) {
      return Collections.emptyList();
    }
    Collections.sort(list);
    return list;
  }

  /**
   * Finds user by user id and returns UserInfo
   *
   * @param userId user id
   * @param params Okapi connection params
   * @return Future with found UserInfo
   */
  private Future<UserInfo> lookupUser(String userId, OkapiConnectionParams params) {
    Promise<UserInfo> promise = Promise.promise();
    RestUtil.doRequest(params, GET_USER_URL + userId, HttpMethod.GET, null)
      .onComplete(getUserResult -> {
        if (RestUtil.validateAsyncResult(getUserResult, promise)) {
          JsonObject response = getUserResult.result().getJson();
          if (!response.containsKey("totalRecords") || !response.containsKey("users")) {
            promise.fail("Error, missing field(s) 'totalRecords' and/or 'users' in user response object");
          } else {
            int recordCount = response.getInteger("totalRecords");
            if (recordCount > 1) {
              String errorMessage = "There are more then one user by requested user id : " + userId;
              LOGGER.error(errorMessage);
              promise.fail(errorMessage);
            } else if (recordCount == 0) {
              String errorMessage = "No user found by user id :" + userId;
              LOGGER.error(errorMessage);
              promise.fail(errorMessage);
            } else {
              JsonObject jsonUser = response.getJsonArray("users").getJsonObject(0);
              JsonObject userPersonalInfo = jsonUser.getJsonObject("personal");
              UserInfo userInfo = new UserInfo()
                .withFirstName(userPersonalInfo.getString("firstName"))
                .withLastName(userPersonalInfo.getString("lastName"))
                .withUserName(jsonUser.getString("username"));
              promise.complete(userInfo);
            }
          }
        }
      });
    return promise.future();
  }

  @Override
  public Future<DataTypeCollection> getDataTypes() {
    Promise<DataTypeCollection> promise = Promise.promise();
    DataTypeCollection dataTypeCollection = new DataTypeCollection();
    dataTypeCollection.setDataTypes(Arrays.asList(DataType.values()));
    dataTypeCollection.setTotalRecords(DataType.values().length);
    promise.complete(dataTypeCollection);
    return promise.future();
  }

  @Override
  public Future<Boolean> isFileExtensionExistByName(FileExtension fileExtension, String tenantId) {
    StringBuilder query = new StringBuilder("extension=" + fileExtension.getExtension().trim());
    if (fileExtension.getId() != null) {
      query.append(" AND id=\"\" NOT id=")
        .append(fileExtension.getId());
    }
    return fileExtensionDao.getFileExtensions(query.toString(), 0, 1, tenantId)
      .compose(collection -> Future.succeededFuture(collection.getTotalRecords() != 0));
  }
}
