package com.example.integration;

import com.example.thl.THLEvent;
import com.example.thl.THLFileWriter;
import com.example.thl.THLFileReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.assertj.core.api.Assertions.*;

import java.io.File;
import java.nio.file.Path;
import java.sql.Timestamp;

/**
 * Integration tests for THL file operations
 */
@DisplayName("THL File Integration Tests")
class THLFileIntegrationTest {
    
    @TempDir
    Path tempDir;
    
    private File thlFile;
    
    @BeforeEach
    void setUp() {
        thlFile = tempDir.resolve("test.thl").toFile();
    }
    
    @Test
    @DisplayName("Should write and read THL events")
    void testWriteAndReadEvents() throws Exception {
        // Create events
        THLEvent event1 = createTestEvent(1L, "event-1", "INSERT");
        THLEvent event2 = createTestEvent(2L, "event-2", "UPDATE");
        THLEvent event3 = createTestEvent(3L, "event-3", "DELETE");
        
        // Write events
        try (THLFileWriter writer = new THLFileWriter(thlFile.getAbsolutePath())) {
            writer.writeEvent(event1);
            writer.writeEvent(event2);
            writer.writeEvent(event3);
        }
        
        // Read events
        try (THLFileReader reader = new THLFileReader(thlFile.getAbsolutePath())) {
            THLEvent readEvent1 = reader.readEvent();
            THLEvent readEvent2 = reader.readEvent();
            THLEvent readEvent3 = reader.readEvent();
            THLEvent readEvent4 = reader.readEvent();
            
            assertThat(readEvent1).isNotNull();
            assertThat(readEvent1.getSeqno()).isEqualTo(1L);
            assertThat(readEvent1.getEventId()).isEqualTo("event-1");
            assertThat(readEvent1.getMetadata().get("event_type")).isEqualTo("INSERT");
            
            assertThat(readEvent2).isNotNull();
            assertThat(readEvent2.getSeqno()).isEqualTo(2L);
            assertThat(readEvent2.getEventId()).isEqualTo("event-2");
            
            assertThat(readEvent3).isNotNull();
            assertThat(readEvent3.getSeqno()).isEqualTo(3L);
            assertThat(readEvent3.getEventId()).isEqualTo("event-3");
            
            assertThat(readEvent4).isNull(); // End of file
        }
    }
    
    @Test
    @DisplayName("Should handle large number of events")
    void testLargeNumberOfEvents() throws Exception {
        int eventCount = 1000;
        
        // Write events
        try (THLFileWriter writer = new THLFileWriter(thlFile.getAbsolutePath())) {
            for (int i = 0; i < eventCount; i++) {
                THLEvent event = createTestEvent((long) i, "event-" + i, "INSERT");
                writer.writeEvent(event);
            }
        }
        
        // Read and count events
        int readCount = 0;
        try (THLFileReader reader = new THLFileReader(thlFile.getAbsolutePath())) {
            THLEvent event;
            while ((event = reader.readEvent()) != null) {
                readCount++;
                assertThat(event.getSeqno()).isEqualTo((long) readCount - 1);
            }
        }
        
        assertThat(readCount).isEqualTo(eventCount);
    }
    
    @Test
    @DisplayName("Should preserve event metadata")
    void testPreserveMetadata() throws Exception {
        THLEvent event = createTestEvent(1L, "event-1", "UPDATE");
        event.addMetadata("table_name", "users");
        event.addMetadata("database_name", "test_db");
        event.addMetadata("binlog_file", "binlog.000001");
        event.addMetadata("binlog_position", 1234L);
        
        // Write event
        try (THLFileWriter writer = new THLFileWriter(thlFile.getAbsolutePath())) {
            writer.writeEvent(event);
        }
        
        // Read event
        try (THLFileReader reader = new THLFileReader(thlFile.getAbsolutePath())) {
            THLEvent readEvent = reader.readEvent();
            
            assertThat(readEvent).isNotNull();
            assertThat(readEvent.getMetadata().get("table_name")).isEqualTo("users");
            assertThat(readEvent.getMetadata().get("database_name")).isEqualTo("test_db");
            assertThat(readEvent.getMetadata().get("binlog_file")).isEqualTo("binlog.000001");
            assertThat(readEvent.getMetadata().get("binlog_position")).isEqualTo(1234L);
        }
    }
    
    @Test
    @DisplayName("Should handle single event")
    void testSingleEvent() throws Exception {
        THLEvent event = createTestEvent(1L, "event-1", "INSERT");
        
        // Write event
        try (THLFileWriter writer = new THLFileWriter(thlFile.getAbsolutePath())) {
            writer.writeEvent(event);
        }
        
        // Read event
        try (THLFileReader reader = new THLFileReader(thlFile.getAbsolutePath())) {
            THLEvent readEvent = reader.readEvent();
            
            assertThat(readEvent).isNotNull();
            assertThat(readEvent.getSeqno()).isEqualTo(1L);
            assertThat(readEvent.getEventId()).isEqualTo("event-1");
            
            // Read again should return null
            THLEvent nextEvent = reader.readEvent();
            assertThat(nextEvent).isNull();
        }
    }
    
    @Test
    @DisplayName("Should handle event with binary data")
    void testBinaryData() throws Exception {
        THLEvent event = createTestEvent(1L, "event-1", "INSERT");
        
        // Set binary data
        byte[] binaryData = new byte[256];
        for (int i = 0; i < 256; i++) {
            binaryData[i] = (byte) i;
        }
        event.setData(binaryData);
        
        // Write event
        try (THLFileWriter writer = new THLFileWriter(thlFile.getAbsolutePath())) {
            writer.writeEvent(event);
        }
        
        // Read event
        try (THLFileReader reader = new THLFileReader(thlFile.getAbsolutePath())) {
            THLEvent readEvent = reader.readEvent();
            
            assertThat(readEvent).isNotNull();
            assertThat(readEvent.getData()).isEqualTo(binaryData);
        }
    }
    
    private THLEvent createTestEvent(long seqno, String eventId, String eventType) {
        THLEvent event = new THLEvent();
        event.setSeqno(seqno);
        event.setEventId(eventId);
        event.setSourceId("mysql-master");
        event.setSourceTstamp(new Timestamp(System.currentTimeMillis()));
        event.addMetadata("event_type", eventType);
        return event;
    }
}
