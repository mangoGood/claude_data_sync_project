package com.example.thl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;
import static org.assertj.core.api.Assertions.*;

import java.sql.Timestamp;
import java.io.*;

/**
 * Unit tests for THLEvent
 */
@DisplayName("THLEvent Tests")
class THLEventTest {
    
    private THLEvent event;
    
    @BeforeEach
    void setUp() {
        event = new THLEvent();
    }
    
    @Test
    @DisplayName("Should set and get seqno")
    void testSeqno() {
        event.setSeqno(12345L);
        assertThat(event.getSeqno()).isEqualTo(12345L);
    }
    
    @Test
    @DisplayName("Should set and get fragno")
    void testFragno() {
        event.setFragno((short) 5);
        assertThat(event.getFragno()).isEqualTo((short) 5);
    }
    
    @Test
    @DisplayName("Should set and get lastFrag")
    void testLastFrag() {
        event.setLastFrag(true);
        assertThat(event.isLastFrag()).isTrue();
        
        event.setLastFrag(false);
        assertThat(event.isLastFrag()).isFalse();
    }
    
    @Test
    @DisplayName("Should set and get sourceId")
    void testSourceId() {
        event.setSourceId("mysql-master-1");
        assertThat(event.getSourceId()).isEqualTo("mysql-master-1");
    }
    
    @Test
    @DisplayName("Should set and get type")
    void testType() {
        event.setType(THLEvent.REPL_DBMS_EVENT);
        assertThat(event.getType()).isEqualTo(THLEvent.REPL_DBMS_EVENT);
    }
    
    @Test
    @DisplayName("Should set and get epochNumber")
    void testEpochNumber() {
        event.setEpochNumber(100L);
        assertThat(event.getEpochNumber()).isEqualTo(100L);
    }
    
    @Test
    @DisplayName("Should set and get sourceTstamp")
    void testSourceTstamp() {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        event.setSourceTstamp(timestamp);
        assertThat(event.getSourceTstamp()).isEqualTo(timestamp);
    }
    
    @Test
    @DisplayName("Should set and get eventId")
    void testEventId() {
        event.setEventId("binlog.000001:1234");
        assertThat(event.getEventId()).isEqualTo("binlog.000001:1234");
    }
    
    @Test
    @DisplayName("Should set and get shardId")
    void testShardId() {
        event.setShardId("shard-1");
        assertThat(event.getShardId()).isEqualTo("shard-1");
    }
    
    @Test
    @DisplayName("Should set and get comment")
    void testComment() {
        event.setComment("Test event");
        assertThat(event.getComment()).isEqualTo("Test event");
    }
    
    @Test
    @DisplayName("Should add and get metadata")
    void testMetadata() {
        event.addMetadata("key1", "value1");
        event.addMetadata("key2", 123);
        
        assertThat(event.getMetadata().get("key1")).isEqualTo("value1");
        assertThat(event.getMetadata().get("key2")).isEqualTo(123);
        assertThat(event.getMetadata()).hasSize(2);
    }
    
    @Test
    @DisplayName("Should set and get data")
    void testData() {
        byte[] data = "test data".getBytes();
        event.setData(data);
        assertThat(event.getData()).isEqualTo(data);
    }
    
    @Test
    @DisplayName("Should serialize and deserialize")
    void testSerialization() throws Exception {
        // Create event with all fields
        event.setSeqno(100L);
        event.setFragno((short) 1);
        event.setLastFrag(true);
        event.setSourceId("mysql-master");
        event.setType(THLEvent.REPL_DBMS_EVENT);
        event.setEpochNumber(50L);
        event.setSourceTstamp(new Timestamp(System.currentTimeMillis()));
        event.setEventId("binlog.000001:1234");
        event.setShardId("shard-1");
        event.setComment("Test event");
        event.addMetadata("key1", "value1");
        event.setData("test data".getBytes());
        
        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(event);
        oos.close();
        
        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        THLEvent deserializedEvent = (THLEvent) ois.readObject();
        ois.close();
        
        // Verify
        assertThat(deserializedEvent.getSeqno()).isEqualTo(100L);
        assertThat(deserializedEvent.getFragno()).isEqualTo((short) 1);
        assertThat(deserializedEvent.isLastFrag()).isTrue();
        assertThat(deserializedEvent.getSourceId()).isEqualTo("mysql-master");
        assertThat(deserializedEvent.getType()).isEqualTo(THLEvent.REPL_DBMS_EVENT);
        assertThat(deserializedEvent.getEpochNumber()).isEqualTo(50L);
        assertThat(deserializedEvent.getEventId()).isEqualTo("binlog.000001:1234");
        assertThat(deserializedEvent.getShardId()).isEqualTo("shard-1");
        assertThat(deserializedEvent.getComment()).isEqualTo("Test event");
        assertThat(deserializedEvent.getMetadata().get("key1")).isEqualTo("value1");
        assertThat(new String(deserializedEvent.getData())).isEqualTo("test data");
    }
    
    @Test
    @DisplayName("Should test event type constants")
    void testEventTypeConstants() {
        assertThat(THLEvent.REPL_DBMS_EVENT).isEqualTo((short) 0);
        assertThat(THLEvent.START_MASTER_EVENT).isEqualTo((short) 1);
        assertThat(THLEvent.STOP_MASTER_EVENT).isEqualTo((short) 2);
        assertThat(THLEvent.HEARTBEAT_EVENT).isEqualTo((short) 3);
    }
}
