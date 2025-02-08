package com.Data.synchronization.service;

import com.Data.synchronization.models.ChangeEvent;
import com.Data.synchronization.models.DatabaseConfig;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import lombok.extern.slf4j.Slf4j;

import javax.swing.text.Document;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class MongoDBCDCHandler {
    private final ChangeEventRepository changeEventRepository;
    private final DatabaseConfig config;
    private final ExecutorService executor;
    private volatile boolean running = false;
    private MongoClient mongoClient;

    public MongoDBCDCHandler(DatabaseConfig config, ChangeEventRepository repository) {
        this.config = config;
        this.changeEventRepository = repository;
        this.executor = Executors.newSingleThreadExecutor();
    }

    public void start() {
        try {
            mongoClient = MongoClients.create(config.getUrl());
            running = true;
            executor.submit(this::watchChanges);
        } catch (Exception e) {
            log.error("Failed to start MongoDB CDC", e);
            throw new RuntimeException(e);
        }
    }

    private void watchChanges() {
        MongoDatabase database = mongoClient.getDatabase(config.getName());

        database.watch()
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .forEach(changeStreamDocument -> {
                    try {
                        processChange(changeStreamDocument);
                    } catch (Exception e) {
                        log.error("Error processing MongoDB change", e);
                    }
                });
    }

    private void processChange(ChangeStreamDocument<Document> changeStreamDocument) {
        ChangeEvent change = new ChangeEvent();
        change.setSourceDb(config.getName());
        change.setTimestamp(Instant.now());
        change.setStatus("PENDING");

        // Set change type based on operation type
        switch (changeStreamDocument.getOperationType()) {
            case INSERT:
                change.setChangeType(ChangeType.INSERT);
                change.setData(convertDocumentToMap(changeStreamDocument.getFullDocument()));
                break;
            case UPDATE:
                change.setChangeType(ChangeType.UPDATE);
                change.setData(convertDocumentToMap(changeStreamDocument.getFullDocument()));
                change.setOldData(convertDocumentToMap(changeStreamDocument.getUpdateDescription().getUpdatedFields()));
                break;
            case DELETE:
                change.setChangeType(ChangeType.DELETE);
                change.setData(convertDocumentToMap(changeStreamDocument.getFullDocument()));
                break;
            default:
                log.warn("Unsupported operation type: {}", changeStreamDocument.getOperationType());
                return;
        }

        change.setTableName(changeStreamDocument.getNamespace().getCollectionName());
        changeEventRepository.save(change);
    }

    private Map<String, Object> convertDocumentToMap(Document document) {
        if (document == null) {
            return new HashMap<>();
        }
        return new HashMap<>(document);
    }

    public void stop() {
        running = false;
        executor.shutdown();
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
