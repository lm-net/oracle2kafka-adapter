package net.matasek.oracle2kafkaadapter.config;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfiguration {

    @Bean
    KafkaProducer<String, String> kafkaProducer(@Value("${kafka.bootstrap-servers}") String bootstrap_servers,
            @Value("${kafka.acks}") String acks,
            @Value("${kafka.retries}") String retries,
            @Value("${kafka.linger-ms}") String linger_ms,
            @Value("${kafka.key-serializer}") String key_serializer,
            @Value("${kafka.value-serializer}") String value_serializer) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap_servers);
        props.put("acks", acks);
        props.put("retries", retries);
        props.put("linger.ms", linger_ms);
        props.put("key.serializer", key_serializer);
        props.put("value.serializer", value_serializer);

        return new KafkaProducer<>(props);
    }
}
