package net.matasek.oracle2kafkaadapter.component;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import net.matasek.oracle2kafkaadapter.model.Topic;
import oracle.jdbc.OracleTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class TopicsBean {

    private static final Logger log = LoggerFactory.getLogger(TopicsBean.class);

    @Autowired
    Connection connection;

    @Bean
    public Map<Long, Topic> getTopics() throws SQLException {
        Map<Long, Topic> topicMap = new HashMap<Long, Topic>();

        CallableStatement stmt = connection.prepareCall("BEGIN :cursor := kf_core.getTopics; END;");
        stmt.registerOutParameter("cursor", OracleTypes.CURSOR);

        stmt.execute();

        ResultSet rs = stmt.getObject("cursor", ResultSet.class);

        while (rs.next()) {
            Topic topic = new Topic();
            topic.setIdTopic(rs.getLong("idTopic"));
            topic.setName(rs.getString("name"));
            topic.setSchemaName(rs.getString("schemaName"));
            topic.setAvroSchema(rs.getString("avroSchema"));

            topicMap.put(topic.getIdTopic(), topic);
        }

        // close cursor
        stmt.close();

        log.info(topicMap.size() + " topics loaded from database");
        
        return topicMap;
    }
}
