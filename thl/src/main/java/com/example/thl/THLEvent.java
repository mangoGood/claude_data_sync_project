package com.example.thl;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;
import java.util.HashMap;

/**
 * THL Event - Transaction History Log Event
 * This class defines a THL event, similar to tungsten-replicator's THLEvent
 */
public class THLEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    // Event types
    public static final short REPL_DBMS_EVENT = 0;
    public static final short START_MASTER_EVENT = 1;
    public static final short STOP_MASTER_EVENT = 2;
    public static final short HEARTBEAT_EVENT = 3;

    // Event fields
    private long seqno;                    // Sequence number
    private short fragno;                  // Fragment number
    private boolean lastFrag;              // Last fragment flag
    private String sourceId;               // Source identifier
    private short type;                    // Event type
    private long epochNumber;              // Epoch number
    private Timestamp sourceTstamp;        // Source timestamp
    private Timestamp localEnqueueTstamp;  // Local enqueue timestamp
    private String comment;                // Comment
    private String eventId;                // Event identifier
    private String shardId;                // Shard ID
    private Map<String, Object> metadata;  // Metadata
    private byte[] data;                   // Raw data

    public THLEvent() {
        this.metadata = new HashMap<>();
        this.type = REPL_DBMS_EVENT;
        this.lastFrag = true;
        this.fragno = 0;
    }

    public THLEvent(long seqno, String eventId, String sourceId) {
        this();
        this.seqno = seqno;
        this.eventId = eventId;
        this.sourceId = sourceId;
        this.sourceTstamp = new Timestamp(System.currentTimeMillis());
    }

    // Getters and Setters
    public long getSeqno() {
        return seqno;
    }

    public void setSeqno(long seqno) {
        this.seqno = seqno;
    }

    public short getFragno() {
        return fragno;
    }

    public void setFragno(short fragno) {
        this.fragno = fragno;
    }

    public boolean isLastFrag() {
        return lastFrag;
    }

    public void setLastFrag(boolean lastFrag) {
        this.lastFrag = lastFrag;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public short getType() {
        return type;
    }

    public void setType(short type) {
        this.type = type;
    }

    public long getEpochNumber() {
        return epochNumber;
    }

    public void setEpochNumber(long epochNumber) {
        this.epochNumber = epochNumber;
    }

    public Timestamp getSourceTstamp() {
        return sourceTstamp;
    }

    public void setSourceTstamp(Timestamp sourceTstamp) {
        this.sourceTstamp = sourceTstamp;
    }

    public Timestamp getLocalEnqueueTstamp() {
        return localEnqueueTstamp;
    }

    public void setLocalEnqueueTstamp(Timestamp localEnqueueTstamp) {
        this.localEnqueueTstamp = localEnqueueTstamp;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getShardId() {
        return shardId == null ? "UNKNOWN" : shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public void addMetadata(String key, Object value) {
        this.metadata.put(key, value);
    }

    public Object getMetadata(String key) {
        return this.metadata.get(key);
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("THLEvent{");
        sb.append("seqno=").append(seqno);
        sb.append(", fragno=").append(fragno);
        sb.append(", lastFrag=").append(lastFrag);
        sb.append(", sourceId='").append(sourceId).append('\'');
        sb.append(", type=").append(type);
        sb.append(", epochNumber=").append(epochNumber);
        sb.append(", sourceTstamp=").append(sourceTstamp);
        sb.append(", eventId='").append(eventId).append('\'');
        sb.append(", shardId='").append(getShardId()).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
