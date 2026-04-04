package com.example.dto;

public class UserEvent {
    private String userId;
    private String eventType;

    public UserEvent() {
    }

    public UserEvent(String userId, String eventType) {
        this.userId = userId;
        this.eventType = eventType;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
}
