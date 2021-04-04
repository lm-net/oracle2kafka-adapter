package net.matasek.oracle2kafkaadapter.config;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OracleConfiguration {

    private static final Logger log = LoggerFactory.getLogger(OracleConfiguration.class);

    @Bean
    public Connection getConnection(@Value("${oracle.url}") String url,
            @Value("${oracle.username}") String username,
            @Value("${oracle.password}") String password) throws SQLException {
        Connection connection = DriverManager.getConnection(url, username, password);

        DatabaseMetaData metadata = connection.getMetaData();
        log.info("Connected to " + metadata.getURL() + "\n" + metadata.getDatabaseProductName() + "\n" + metadata.getDatabaseProductVersion());

        connection.setAutoCommit(false);
        connection.setClientInfo("OCSID.MODULE", "Oracle2Kafka Adapter");
        
        return connection;
    }
}
