package com.example.dto;

public class Event {
    private String eventId;
    private String userId;
    private String payload;

    public Event() {
    }

    public Event(String eventId, String userId, String payload) {
        this.eventId = eventId;
        this.userId = userId;
        this.payload = payload;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "Event{"
                + "eventId='" + eventId + '\''
                + ", userId='" + userId + '\''
                + ", payload='" + payload + '\''
                + '}';
    }
}
