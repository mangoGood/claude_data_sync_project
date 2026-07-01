package com.migration.capture;

import com.migration.common.AbstractCapture;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.BSONTimestamp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class MongoOplogCapture extends AbstractCapture<byte[]> {

    private String host;
    private int port;
    private String user;
    private String password;
    private String database;
    private String outputDir;
    private String replicaSet;

    private MongoClient mongoClient;
    private MongoCollection<Document> oplogCollection;
    private FileOutputStream outputStream;
    private long eventCount = 0;
    private AtomicBoolean tailing = new AtomicBoolean(false);

    private BSONTimestamp lastTimestamp;

    @Override
    protected void doInitialize() throws Exception {
        host = props.getProperty("mongo.host", "localhost");
        port = Integer.parseInt(props.getProperty("mongo.port", "27017"));
        user = props.getProperty("mongo.user", "");
        password = props.getProperty("mongo.password", "");
        database = props.getProperty("mongo.database", "admin");
        replicaSet = props.getProperty("mongo.replica.set", "");

        outputDir = props.getProperty("output.dir", "./output");

        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }

        logger.info("MongoDB Oplog Capture initialized - host: {}, port: {}, database: {}", host, port, database);
    }

    @Override
    protected void doStart() throws Exception {
        ConnectionString connectionString = buildConnectionString();
        MongoClientSettings settings = MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .build();

        mongoClient = MongoClients.create(settings);

        MongoDatabase localDb = mongoClient.getDatabase("local");
        oplogCollection = localDb.getCollection("oplog.rs");

        String outputFileName = "oplog-" + System.currentTimeMillis() + ".bin";
        File outputFile = new File(outputDir, outputFileName);
        outputStream = new FileOutputStream(outputFile);
        logger.info("Output file: {}", outputFile.getAbsolutePath());

        tailing.set(true);
        tailOplog();

        logger.info("MongoDB Oplog Capture started successfully");
    }

    @Override
    protected void doStop() throws Exception {
        tailing.set(false);

        if (mongoClient != null) {
            try {
                logger.info("Closing MongoDB connection...");
                mongoClient.close();
            } catch (Exception e) {
                logger.error("Error closing MongoDB connection", e);
            }
        }

        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                logger.error("Error closing output stream", e);
            }
        }

        logger.info("MongoDB Oplog Capture stopped. Total events captured: {}", eventCount);
    }

    private ConnectionString buildConnectionString() {
        StringBuilder sb = new StringBuilder("mongodb://");

        if (user != null && !user.isEmpty()) {
            sb.append(user);
            if (password != null && !password.isEmpty()) {
                sb.append(":").append(password);
            }
            sb.append("@");
        }

        sb.append(host).append(":").append(port);

        if (replicaSet != null && !replicaSet.isEmpty()) {
            sb.append("/?replicaSet=").append(replicaSet);
        }

        return new ConnectionString(sb.toString());
    }

    private void tailOplog() {
        try {
            Bson filter = lastTimestamp != null
                ? Filters.gt("ts", lastTimestamp)
                : new Document();

            FindIterable<Document> findIterable = oplogCollection
                .find(filter)
                .cursorType(CursorType.TailableAwait)
                .oplogReplay(true)
                .noCursorTimeout(true);

            try (MongoCursor<Document> cursor = findIterable.iterator()) {
                while (cursor.hasNext() && tailing.get()) {
                    Document oplogEntry = cursor.next();
                    processOplogEntry(oplogEntry);
                }
            }
        } catch (Exception e) {
            if (tailing.get()) {
                logger.error("Error tailing oplog", e);
            }
        }
    }

    private void processOplogEntry(Document oplogEntry) {
        if (!running) {
            return;
        }

        try {
            String operation = oplogEntry.getString("op");
            String namespace = oplogEntry.getString("ns");
            BSONTimestamp timestamp = oplogEntry.get("ts", BSONTimestamp.class);
            Document object = oplogEntry.get("o", Document.class);
            Document update = oplogEntry.get("o2", Document.class);

            if (operation == null || "n".equals(operation)) {
                return;
            }

            lastTimestamp = timestamp;

            byte[] oplogBytes = serializeOplogEntry(oplogEntry);

            if (oplogBytes != null && oplogBytes.length > 0) {
                outputStream.write(oplogBytes);
                outputStream.flush();

                eventCount++;

                currentPosition = "ts:" + timestamp.getTime() + ":" + timestamp.getInc();

                if (eventCount % 100 == 0) {
                    logger.info("Captured {} oplog events, current position: {}", eventCount, currentPosition);
                }
            }
        } catch (IOException e) {
            logger.error("Error writing oplog entry to output file", e);
        }
    }

    private byte[] serializeOplogEntry(Document oplogEntry) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("OPLOG:");

            String operation = oplogEntry.getString("op");
            String namespace = oplogEntry.getString("ns");
            BSONTimestamp timestamp = oplogEntry.get("ts", BSONTimestamp.class);

            sb.append(operation).append(":");
            sb.append(timestamp.getTime()).append(":");
            sb.append(timestamp.getInc()).append(":");
            sb.append(namespace).append("\n");

            return sb.toString().getBytes("UTF-8");
        } catch (Exception e) {
            logger.error("Error serializing oplog entry", e);
            return null;
        }
    }

    private String getOperationName(String operation) {
        switch (operation) {
            case "i":
                return "INSERT";
            case "u":
                return "UPDATE";
            case "d":
                return "DELETE";
            case "c":
                return "COMMAND";
            default:
                return operation;
        }
    }

    public long getEventCount() {
        return eventCount;
    }
}
