package net.matasek.oracle2kafkaadapter.model;

public class Event {

    private Long idEvent;
    private Long idTopic;
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
     * @return the idTopic
     */
    public Long getIdTopic() {
        return idTopic;
    }

    /**
     * @param idTopic the idTopic to set
     */
    public void setIdTopic(Long idTopic) {
        this.idTopic = idTopic;
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
        return "idEvent: " + idEvent + "\nidTopic: " + idTopic + "\nkey: " + key + "\nmessage: " + message;
    }

}
