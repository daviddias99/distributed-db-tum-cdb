package de.tum.i13.server.persistentstorage.btree.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * This class contains several utilities for operations with the file system. It
 * handles creating/deleting directories, reading/writing objects to disk, and
 * copying/deleting files
 */
public class StorageUtils {
    private static final Logger LOGGER = LogManager.getLogger(StorageUtils.class);

    /**
     * Copies file at {@code src} to {@code dst}, replacing if {@code dst} already
     * exists
     * 
     * @param src path to the source location
     * @param dst path to the destination location
     * @throws IOException See
     *                     {@link java.nio.file.Files#copy(Path, Path, CopyOption...)}
     */
    public static void copyAndReplaceFile(Path src, Path dst) throws IOException {
        Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Create a new directory
     * 
     * @param storageFolder path of the new directory
     */
    public static void createDirectory(Path storageFolder) {
        File theDir = storageFolder.toFile();
        if (!theDir.exists()) {
            theDir.mkdirs();
        }
    }

    /**
     * Delete a directory
     * 
     * @param directoryToBeDeleted directory to be deleted
     * @return true if directory was deleted successfuly, false otherwise
     * @throws IOException
     */
    private static void deleteDirectory(Path directoryToBeDeleted) throws IOException {
        File[] allContents = directoryToBeDeleted.toFile().listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file.toPath());
            }
        }

        Files.delete(directoryToBeDeleted);
    }

    /**
     * Read object from disk
     * 
     * @param filePath path to the object in disk
     * @param <V>      Type of object to read
     * @return read object
     * @throws StorageException an exception is thrown if either the file can't be
     *                          found or an error occured while reading
     */
    public static <V> V readObject(Path filePath) throws StorageException {
        try (FileInputStream fileIn = new FileInputStream(filePath.toString())) {
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
            @SuppressWarnings("unchecked")
            V obj = (V) objectIn.readObject();
            objectIn.close();
            return obj;
        } catch (FileNotFoundException e) {
            throw new StorageException(e,
                    "Throwing exception because the file %s could not be found.", filePath);
        } catch (IOException e) {
            throw new StorageException(e, "I/O error while reading object from memory %s", filePath);
        } catch (ClassNotFoundException e) {
            throw new StorageException(e,
                    "Unknown error while reading object from memory");
        }
    }

    /**
     * 
     * @param filePath path where the object will be stored
     * @param obj      object to store
     * @param <V>      Type of object to write
     * @throws StorageException an exception is thrown if either the file can't be
     *                          found or an error occured while writing
     */
    public static <V> void writeObject(Path filePath, V obj) throws StorageException {
        try (FileOutputStream fileOut = new FileOutputStream(filePath.toString())) {
            var objectOut = new ObjectOutputStream(fileOut);
            objectOut.writeObject(obj);
            objectOut.close();
        } catch (FileNotFoundException e) {
            throw new StorageException(e,
                    "Throwing exception because the file %s could not be found.", filePath);
        } catch (IOException e) {
            throw new StorageException(e, "I/O error while writing object to disk");
        }
    }

    /**
     * Delete file from disk
     * 
     * @param filePath path to file
     * @throws StorageException an exception is thrown for unexpected I/O errors
     */
    public static void deleteFile(Path filePath) throws StorageException {
        try {

            if (filePath.toFile().isDirectory()) {
                deleteDirectory(filePath);
                return;
            }

            Files.deleteIfExists(filePath);
            LOGGER.debug("Deleted chunk ({}) from disk.", filePath);
        } catch (IOException e) {
            throw new StorageException(e, "I/O error while deleting file from disk");
        }
    }
}
