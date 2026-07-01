package com.example.thl;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class THLFileReader implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(THLFileReader.class);

    private FileInputStream fis;
    private ObjectInputStream ois;

    public THLFileReader(String thlFile) throws IOException {
        File file = new File(thlFile);
        if (!file.exists()) {
            throw new IOException("THL file not found: " + thlFile);
        }

        fis = new FileInputStream(file);
        ois = new ObjectInputStream(fis);

        logger.info("Opened THL file: {}", thlFile);
    }

    public THLEvent readEvent() throws IOException, ClassNotFoundException {
        try {
            return (THLEvent) ois.readObject();
        } catch (java.io.EOFException e) {
            // End of file reached
            return null;
        } catch (IOException e) {
            // End of file reached or other IO error
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        if (ois != null) {
            ois.close();
        }
        if (fis != null) {
            fis.close();
        }
        logger.info("Closed THL file reader");
    }
}
