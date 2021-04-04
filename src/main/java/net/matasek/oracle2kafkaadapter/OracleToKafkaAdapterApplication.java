package net.matasek.oracle2kafkaadapter;

import net.matasek.oracle2kafkaadapter.service.Adapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OracleToKafkaAdapterApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(OracleToKafkaAdapterApplication.class);

    @Autowired
    Adapter adapter;
    
    public static void main(String[] args) {
        SpringApplication.run(OracleToKafkaAdapterApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        adapter.run();
    }

}
