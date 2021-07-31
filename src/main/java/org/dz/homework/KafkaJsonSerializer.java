package org.dz.homework;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;

public class KafkaJsonSerializer implements Serializer<OutputMessage> {

    private Logger logger = LogManager.getLogger(this.getClass());

    @Override
    public void configure(Map map, boolean b) {}

    @Override
    public byte[] serialize(String s, OutputMessage outputMessage) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsBytes(outputMessage);
        } catch (Exception e) {
            logger.error("failed serializing JsonMessage", e);
        }
        return retVal;
    }

    @Override
    public byte[] serialize(String topic, Headers headers, OutputMessage outputMessage) {
        try {
            return new ObjectMapper().writeValueAsBytes(outputMessage) ;
        } catch (JsonProcessingException e) {
            logger.error("failed serializing JsonMessage", e);
            return null;
        }
    }

    @Override
    public void close() { }
}
