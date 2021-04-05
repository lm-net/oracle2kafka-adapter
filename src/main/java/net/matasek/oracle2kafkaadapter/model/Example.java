package net.matasek.oracle2kafkaadapter.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.matasek.oracle2kafkaadapter.model.avro.ExampleAvro;

public class Example implements AvroObject {

    private String name;
    private String surname;
    private Integer age;

    @JsonCreator
    public Example(@JsonProperty("name") String name, @JsonProperty("surname") String surname) {
        this.name = name;
        this.surname = surname;
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
     * @return the surname
     */
    public String getSurname() {
        return surname;
    }

    /**
     * @param surname the surname to set
     */
    public void setSurname(String surname) {
        this.surname = surname;
    }

    /**
     * @return the age
     */
    public Integer getAge() {
        return age;
    }

    /**
     * @param age the age to set
     */
    public void setAge(Integer age) {
        this.age = age;
    }

    public String toString() {
        return "name: " + name + ", surname: " + surname;
    }
    
    public Object getAvroObject() {
        ExampleAvro avro = new ExampleAvro();
        avro.setName(name);
        avro.setSurname(surname);
        avro.setAge(age);
        
        return avro;
    }
}
