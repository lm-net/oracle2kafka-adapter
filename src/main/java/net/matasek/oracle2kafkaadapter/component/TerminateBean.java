package net.matasek.oracle2kafkaadapter.component;

import java.sql.Connection;
import java.sql.SQLException;
import javax.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TerminateBean {

    private static final Logger log = LoggerFactory.getLogger(TerminateBean.class);

    @Autowired
    Connection connection;

    @Autowired
    private KafkaProducer<String, String> kafkaProducer;

    @PreDestroy
    public void onDestroy() throws Exception {
        try {
            connection.rollback();
            connection.close();
            log.info("Database closed");
        } catch (SQLException exception) {
            log.error("Database cannot be closed\n" + exception.getMessage());
        }

        kafkaProducer.close();
        log.info("Kafka producer closed");
    }
}
