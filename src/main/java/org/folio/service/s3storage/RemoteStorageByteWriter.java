package org.folio.service.s3storage;

import java.io.*;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.s3.client.FolioS3Client;
import org.folio.s3.exception.S3ClientException;

public class RemoteStorageByteWriter {

    private final File tmp;
    private final String path;

    private final BufferedOutputStream bufferedOutputStream;
    private final FolioS3Client s3Client;

    public RemoteStorageByteWriter(String path, int size, FolioS3Client s3Client) {
      try {
        this.s3Client = s3Client;
        this.path = path;

        this.tmp = Files.createTempFile(FilenameUtils.getName(path), FilenameUtils.getExtension(path))
          .toFile();

        this.bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(this.tmp));

      } catch (Exception ex) {
        throw new S3ClientException("Files buffer cannot be created due to error: " + ex.getMessage());
      }
    }

    public void write(byte[] buffer, int offset, int numberBytes) {
    if (numberBytes > 0) {
      try {
        bufferedOutputStream.write(buffer, offset, numberBytes);
      } catch (IOException e) {
        deleteTmp(tmp);
      }
    } else {
      deleteTmp(tmp);
    }
  }

    public void close() {
      try {
        if (tmp.exists()) {
          bufferedOutputStream.close();
          s3Client.write(path, FileUtils.openInputStream(tmp));
        }
      } catch (Exception ex) {
        throw new S3ClientException("Error while close(): " + ex.getMessage());
      } finally {
        deleteTmp(tmp);
      }
    }

    private void deleteTmp(File tmp) {
      try {
        Files.deleteIfExists(tmp.toPath());
      } catch (IOException ex) {
        throw new S3ClientException("Error in deleting file: " + ex.getMessage());
      }
    }
  }
