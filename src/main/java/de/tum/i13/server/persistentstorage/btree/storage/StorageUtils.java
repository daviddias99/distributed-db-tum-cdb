package de.tum.i13.server.persistentstorage.btree.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.shared.Constants;

public class StorageUtils {
  private static final Logger LOGGER = LogManager.getLogger(StorageUtils.class);

  public static void copyAndReplaceFile(Path src, Path dst) throws IOException {
    Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
  }

  public static void createDirectory(Path storageFolder) {
    File theDir = storageFolder.toFile();
    if (!theDir.exists()) {
      theDir.mkdirs();
    }
  }

  public static boolean deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        deleteDirectory(file);
      }
    }
  
    return directoryToBeDeleted.delete();
  }

  public static Object readObject(Path filePath) throws StorageException {
    try (FileInputStream fileIn = new FileInputStream(filePath.toString())) {
      ObjectInputStream objectIn = new ObjectInputStream(fileIn);
      Object obj = objectIn.readObject();
      objectIn.close();
      return obj;
    } catch (FileNotFoundException e) {
      StorageException storageException = new StorageException(e,
          "Throwing exception because the file %s could not be found.", filePath);
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
      throw storageException;
    } catch (IOException e) {
      StorageException storageException = new StorageException(e, "I/O error while reading object from memory");
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
      throw storageException;
    } catch (ClassNotFoundException e) {
      StorageException storageException = new StorageException(e, "Unknown error while reading object from memory");
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
      throw storageException;
    }
  }

  public static void writeObject(Path filePath, Object obj) throws StorageException {
    try (FileOutputStream fileOut = new FileOutputStream(filePath.toString())) {
      var objectOut = new ObjectOutputStream(fileOut);
      objectOut.writeObject(obj);
      objectOut.close();
    } catch (FileNotFoundException e) {
      StorageException storageException = new StorageException(e,
          "Throwing exception because the file %s could not be found.", filePath);
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
      throw storageException;
    } catch (IOException e) {
      StorageException storageException = new StorageException(e, "I/O error while writing object to disk");
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
      throw storageException;
    }
  }

  public static void deleteFile(Path filePath) throws StorageException {
    try {
      Files.delete(filePath);
      LOGGER.debug("Deleted chunk ({}) from disk.", filePath);
    } catch (IOException e) {
      StorageException storageException = new StorageException(e, "I/O error while deleting file from disk");
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
      throw storageException;
    }
  }
}
