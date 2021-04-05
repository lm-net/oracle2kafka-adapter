package net.matasek.oracle2kafkaadapter.model;

import org.apache.avro.Schema;

public class Topic {

    private Long idTopic;
    private String name;
    private String schemaName;
    private String avroSchema;
    private Schema schema;

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
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
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
     * @return the avroSchema
     */
    public String getAvroSchema() {
        return avroSchema;
    }

    /**
     * @param avroSchema the avroSchema to set
     */
    public void setAvroSchema(String avroSchema) {
        this.avroSchema = avroSchema;

        if (avroSchema != null) {
            // vytvori genericky parser pro Avro
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(avroSchema);
        }
    }

    /**
     * @return the schema
     */
    public Schema getSchema() {
        return schema;
    }
}
