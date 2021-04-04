package net.matasek.oracle2kafkaadapter.model;

public class Event {

    private Long idEvent;
    private String topic;
    private String schemaName;
    private String key;
    private String message;

    /**
     * @return the idEvent
     */
    public Long getIdEvent() {
        return idEvent;
    }

    /**
     * @param idEvent the idEvent to set
     */
    public void setIdEvent(Long idEvent) {
        this.idEvent = idEvent;
    }

    /**
     * @return the topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * @param topic the topic to set
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @param schemaName the schemaName to set
     */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * @param key the key to set
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * @return the message
     */
    public String getMessage() {
        return message;
    }

    /**
     * @param message the message to set
     */
    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "idEvent: " + idEvent + "\ntopic: " + topic + "\nschemaName: " + schemaName + "\nkey: " + key + "\nmessage: " + message;
    }
}
