package com.example.thl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class THLFileWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(THLFileWriter.class);

    private File thlFile;
    private FileOutputStream fos;
    private ObjectOutputStream oos;

    public THLFileWriter(String filePath) throws IOException {
        this.thlFile = new File(filePath);
        
        // Create parent directory if it doesn't exist
        File parentDir = thlFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        fos = new FileOutputStream(thlFile);
        oos = new ObjectOutputStream(fos);

        logger.info("Created THL file: {}", thlFile.getAbsolutePath());
    }

    public void writeEvent(THLEvent event) throws IOException {
        oos.writeObject(event);
        oos.flush();
    }

    @Override
    public void close() throws IOException {
        if (oos != null) {
            oos.close();
        }
        if (fos != null) {
            fos.close();
        }
        logger.info("Closed THL file writer");
    }
}
