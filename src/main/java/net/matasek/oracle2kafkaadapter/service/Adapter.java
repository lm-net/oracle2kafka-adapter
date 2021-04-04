package net.matasek.oracle2kafkaadapter.service;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import net.matasek.oracle2kafkaadapter.model.Event;
import oracle.jdbc.OracleTypes;
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
    private int numberOfEventsReadInOneLoop;
    @Value("${adapter.sleepTimeWhenEventQueueHasEvents}")
    private int sleepTimeWhenEventQueueHasEvents;
    @Value("${adapter.sleepTimeWhenEventQueueIsEmpty}")
    private int sleepTimeWhenEventQueueIsEmpty;

    @Autowired
    private Connection connection;

    @Autowired
    private KafkaProducer<String, String> kafkaProducer;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    private final List<Event> events;
    private final List<Long> eventIdsSent;
    private final List<Long> eventIdsFailed;

    public Adapter() {
        events = new ArrayList<>();
        eventIdsSent = new ArrayList<>();
        eventIdsFailed = new ArrayList<>();
    }

    public void run() throws SQLException {
        log.info("Adapter.run");
        AvailabilityChangeEvent.publish(eventPublisher, this, LivenessState.CORRECT);
        AvailabilityChangeEvent.publish(eventPublisher, this, ReadinessState.ACCEPTING_TRAFFIC);

        // infinite loop
        while (true) {
            events.clear();
            eventIdsSent.clear();
            eventIdsFailed.clear();

            readEventsFromDatabase();
            sendEventsToKafka();
            confirmEventsToDatabase();

            // sleep when queue is empty
            int sleepTime = events.size() < numberOfEventsReadInOneLoop ? sleepTimeWhenEventQueueIsEmpty : sleepTimeWhenEventQueueHasEvents;

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
        stmt.setInt("requiredEventsCount", numberOfEventsReadInOneLoop);

        stmt.execute();

        ResultSet rs = stmt.getObject("cursor", ResultSet.class
        );

        while (rs.next()) {
            Event event = new Event();
            event.setIdEvent(rs.getLong("idevent"));
            event.setTopic(rs.getString("topic"));
            event.setSchemaName(rs.getString("schemaName"));
            event.setKey(rs.getString("key"));
            event.setMessage(rs.getString("message"));

            events.add(event);
        }

        // close cursor
        stmt.close();

        log.info(events.size() + " messages read from database");
    }

    private void confirmEventsToDatabase() {
        log.info("Adapter.confirmEventsToDatabase");
    }

    private void sendEventsToKafka() {
        log.info("Adapter.sendEventsToKafka");

        for (Event event : events) {
            kafkaProducer.send(new ProducerRecord<>(event.getTopic(), event.getKey(), event.getMessage()), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        eventIdsSent.add(event.getIdEvent());
                    } else {
                        eventIdsFailed.add(event.getIdEvent());
                        log.error("Message failed to be sent to Kafka\n" + exception.getMessage() + "\n" + event.toString());
                    }
                }
            });
        }

        kafkaProducer.flush();
        log.info("Messages flushed to Kafka");
    }
}
