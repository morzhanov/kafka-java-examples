package com.example.kafka.streams;

public class GithubEventAggregate {
    public GithubEventAggregate(String eventType, String startTimestamp, String endTimestamp, long count,
            String createdAt) {
        this.eventType = eventType;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.count = count;
        this.createdAt = createdAt;
    }

    public String eventType;
    public String startTimestamp;
    public String endTimestamp;
    public long count;
    public String createdAt;
}
