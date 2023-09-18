package org.folio.service.processing.split;

import static org.folio.service.processing.reader.MarcJsonReader.JSON_EXTENSION;
import static org.folio.service.processing.reader.MarcXmlReader.XML_EXTENSION;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import lombok.experimental.UtilityClass;
import org.apache.commons.io.FilenameUtils;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.service.processing.ParallelFileChunkingProcessor;

@UtilityClass
public class FileSplitUtilities {

  public static final byte MARC_RECORD_TERMINATOR = (byte) 0x1d;

  /**
   * Creates the S3 key for a split chunk within a larger file
   */
  public static String buildChunkKey(String baseKey, int partNumber) {
    String[] keyNameParts = baseKey.split("\\.");

    if (keyNameParts.length > 1) {
      String partUpdate = String.format(
        "%s_%s",
        keyNameParts[keyNameParts.length - 2],
        partNumber
      );
      keyNameParts[keyNameParts.length - 2] = partUpdate;
      return String.join(".", keyNameParts);
    }

    return String.format("%s_%s", baseKey, partNumber);
  }

  /**
   * Counts records in a given {@link InputStream}, <strong>closing it afterwards</strong>.
   *
   * @throws IOException if the stream cannot be read or a temp file cannot be created
   */
  public static int countRecordsInFile(
    String filename,
    InputStream inStream,
    JobProfileInfo profile
  ) throws IOException {
    File tempFile = Files
      .createTempFile(
        "di-tmp-",
        // later stage requires correct file extension
        Path.of(filename).getFileName().toString(),
        PosixFilePermissions.asFileAttribute(
          PosixFilePermissions.fromString("rwx------")
        )
      )
      .toFile();

    try (
      InputStream autoCloseMe = inStream;
      OutputStream fileOutputStream = new FileOutputStream(tempFile)
    ) {
      inStream.transferTo(fileOutputStream);
      fileOutputStream.flush();

      return ParallelFileChunkingProcessor.countTotalRecordsInFile(
        tempFile,
        profile
      );
    } finally {
      Files.deleteIfExists(tempFile.toPath());
    }
  }

  public static Path createTemporaryDir(String key) throws IOException {
    return Files.createTempDirectory(
      String.format("di-split-%s", key.replace('/', '-')),
      PosixFilePermissions.asFileAttribute(
        PosixFilePermissions.fromString("rwx------")
      )
    );
  }

  public boolean isMarcBinary(String path, JobProfileInfo profile) {
    if (profile.getDataType() != JobProfileInfo.DataType.MARC) {
      return false;
    }

    String extension = FilenameUtils.getExtension(path);

    return (
      !JSON_EXTENSION.equals(extension) && !XML_EXTENSION.equals(extension)
    );
  }
}
