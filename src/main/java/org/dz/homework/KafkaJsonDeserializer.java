package org.dz.homework;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * deserializes received messages into an InputMessage
 * @param <T>
 */
public class KafkaJsonDeserializer<T> implements Deserializer<InputMessage> {

    private Logger logger = LogManager.getLogger(this.getClass());
    private ObjectMapper objectMapper = new ObjectMapper();

    public KafkaJsonDeserializer() {
    }

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public InputMessage deserialize(String s, byte[] bytes) {
        try {
            InputMessage inputMessage = objectMapper.readValue(new String(bytes, "UTF-8"), InputMessage.class);
            return inputMessage;
        } catch (Exception e) {
            logger.error("Unable to deserialize message", e);
            return null;
        }
    }

    @Override
    public InputMessage deserialize(String topic, Headers headers, byte[] data) {
        try {
            InputMessage inputMessage = objectMapper.readValue(new String(data, "UTF-8"), InputMessage.class);
            return inputMessage;
        } catch (Exception e) {
            logger.error("Unable to deserialize message", e);
            return null;
        }
    }

    @Override
    public void close() {

    }
}