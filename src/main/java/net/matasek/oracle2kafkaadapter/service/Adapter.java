package net.matasek.oracle2kafkaadapter.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.matasek.oracle2kafkaadapter.model.AvroObject;
import net.matasek.oracle2kafkaadapter.model.Event;
import net.matasek.oracle2kafkaadapter.model.Topic;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

@Service
public class Adapter {

    private static final Logger log = LoggerFactory.getLogger(Adapter.class);

    @Value("${adapter.numberOfEventsReadInOneLoop}")
    private int countOfEventsReadInOneLoop;
    @Value("${adapter.sleepTimeWhenEventQueueHasEvents}")
    private int sleepTimeWhenEventQueueHasEvents;
    @Value("${adapter.sleepTimeWhenEventQueueIsEmpty}")
    private int sleepTimeWhenEventQueueIsEmpty;
    @Value("${adapter.avroSchemaGeneric}")
    private String avroSchemaGeneric;

    @Autowired
    private Connection connection;

    @Autowired
    private KafkaProducer<String, String> kafkaProducerJson;
    @Autowired
    private KafkaProducer<String, Object> kafkaProducerAvro;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    @Autowired
    private Map<Long, Topic> topics;

    private final List<Event> events;
    private final List<Long> eventIdsSent;
    private final List<Long> eventIdsFailed;

    public Adapter() {
        events = new ArrayList<>();
        eventIdsSent = new ArrayList<>();
        eventIdsFailed = new ArrayList<>();
    }

    public void run() {
        log.info("Adapter.run");
        AvailabilityChangeEvent.publish(eventPublisher, this, LivenessState.CORRECT);
        AvailabilityChangeEvent.publish(eventPublisher, this, ReadinessState.ACCEPTING_TRAFFIC);

        // infinite loop
        while (true) {
            events.clear();
            eventIdsSent.clear();
            eventIdsFailed.clear();

            int sleepTime;

            try {
                readEventsFromDatabase();
                sendEventsToKafka();
                confirmEventsToDatabase();

                // sleep when queue is empty
                sleepTime = events.size() < countOfEventsReadInOneLoop ? sleepTimeWhenEventQueueIsEmpty : sleepTimeWhenEventQueueHasEvents;
            } catch (Exception ex) {
                log.error(ex.getMessage());

                // sleep time when unexpected error occurs
                sleepTime = 10000;
            }

            try {
                if (sleepTime > 0) {
                    log.info("sleep for " + sleepTime + " ms");
                    Thread.sleep(sleepTime);
                }
            } catch (InterruptedException ex) {
                log.error(ex.getMessage());
            }
        }
    }

    private void readEventsFromDatabase() throws SQLException {
        log.info("Adapter.readEventsFromDatabase");

        CallableStatement stmt = connection.prepareCall("BEGIN :cursor := kf_core.readEvents(a_requiredeventscount => :requiredEventsCount); END;");
        stmt.registerOutParameter("cursor", OracleTypes.CURSOR);
        stmt.setInt("requiredEventsCount", countOfEventsReadInOneLoop);

        stmt.execute();

        ResultSet rs = stmt.getObject("cursor", ResultSet.class
        );

        while (rs.next()) {
            Event event = new Event();
            event.setIdEvent(rs.getLong("idEvent"));
            event.setIdTopic(rs.getLong("idTopic"));
            event.setKey(rs.getString("key"));
            event.setMessage(rs.getString("message"));

            events.add(event);
        }

        // close cursor
        stmt.close();

        log.info(events.size() + " messages read from database");
    }

    private void confirmEventsToDatabase() throws SQLException {
        log.info("Adapter.confirmEventsToDatabase");

        OracleConnection oracleConnection = (OracleConnection) connection;

        Array successfullySent = oracleConnection.createOracleArray("KF_IDS", eventIdsSent.toArray());
        Array failedToSend = oracleConnection.createOracleArray("KF_IDS", eventIdsFailed.toArray());

        CallableStatement stmt = connection.prepareCall("BEGIN kf_core.confirmEvents(a_successfullySent => :successfullySent, a_failedToSend => :failedToSend); END;");
        stmt.setArray(1, successfullySent);
        stmt.setArray(2, failedToSend);

        stmt.execute();

        stmt.close();
    }

    private void sendEventsToKafka() {
        log.info("Adapter.sendEventsToKafka");

        for (Event event : events) {
            Callback callback = new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        eventIdsSent.add(event.getIdEvent());
                    } else {
                        eventIdsFailed.add(event.getIdEvent());
                        log.error("Message failed to be sent to Kafka\n" + exception.getMessage() + "\n" + event.toString());
                    }
                }
            };

            Topic topic = topics.get(event.getIdTopic());

            try {
                // message ve formátu JSON
                if (topic.getSchemaName() == null) {
                    kafkaProducerJson.send(new ProducerRecord<>(topic.getName(), event.getKey(), event.getMessage()), callback);
                } // message ve formátu AVRO (generickém - plochá struktura, negenerujeme POJO třídu)
                else if (topic.getSchemaName().equals(avroSchemaGeneric)) {
                    // rozparsuje JSON do mapy
                    Map<String, String> map = (new ObjectMapper()).readValue(event.getMessage(), new TypeReference<HashMap<String, String>>() {
                    });

                    // nasetuje hodnoty pro jeden genericky record
                    GenericRecord record = new GenericData.Record(topic.getSchema());
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        record.put(entry.getKey(), entry.getValue());
                    }

                    kafkaProducerAvro.send(new ProducerRecord<>(topic.getName(), event.getKey(), record), callback);
                } // message ve formátu AVRO (strukturovaná - zanořené struktury, je nutné vygenerovat POJO)
                else {
                    AvroObject object = (new ObjectMapper()).readerFor(Class.forName(topic.getSchemaName())).readValue(event.getMessage());

                    kafkaProducerAvro.send(new ProducerRecord<>(topic.getName(), event.getKey(), object.getAvroObject()), callback);
                }
            } catch (JsonProcessingException ex) {
                eventIdsFailed.add(event.getIdEvent());
                log.error("JSON failed to be parsed - eventId: " + event.getIdEvent());
                log.error(event.getMessage());
            } catch (ClassNotFoundException ex) {
                eventIdsFailed.add(event.getIdEvent());
                log.error("Class not found - idTopic: " + topic.getIdTopic() + ", schemaName: " + topic.getSchemaName());
            }
        }

        kafkaProducerJson.flush();
        log.info("Messages flushed to Kafka");
    }
}
