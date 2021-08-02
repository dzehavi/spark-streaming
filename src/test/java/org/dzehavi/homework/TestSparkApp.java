package org.dzehavi.homework;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.dz.homework.JavaSparkApp;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TestSparkApp {

    private static KafkaProducer<String, String> producer;
    private static Consumer<String, String> consumer;

    @BeforeClass
    public static void init() throws InterruptedException {

        // start producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create a producer for input messages
        producer =
                new KafkaProducer<>(
                        producerProps,
                        new StringSerializer(),
                        new StringSerializer()
                );

        // start consumer
        final Properties consumerProps = new Properties();
        consumerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group_test2");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // Create the consumer using consumerProps.
        consumer = new KafkaConsumer<>(consumerProps);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList("my-cdc-output-topic"));

        // start the application
        new Thread(() -> {
            try {
                JavaSparkApp.main(new String[0]);
            } catch (InterruptedException e) {
                assert false;
            }
        }).start();


    }

    @Test
    public void testInsert() throws ExecutionException, InterruptedException {
        producer.send(new ProducerRecord<>("my-cdc-input-topic",
                "{\"pk\":\"123456\",\"data\":{\"a\":\"foo\",\"b\":\"bar\",\"c\":123},\"beforeData\":null,\"headers\":{\"operation\":\"INSERT\",\"timestamp\":\"2021-01-27T13:42:13.383\",\"streamPosition\":\"123154689132138433181312132194984313218\"}}"));
        ConsumerRecords<String, String> messages = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> message : messages) {
            assert "{\"data\":{\"a\":\"foo\",\"b\":\"bar\",\"c\":123}}".equalsIgnoreCase(message.value());
            return;
        }
    }

    @Test
    public void testUpdate() throws ExecutionException, InterruptedException {
        producer.send(new ProducerRecord<>("my-cdc-input-topic",
                "{\"pk\":\"123456\",\"data\":{\"a\":\"foo\",\"b\":\"bar\",\"c\":123},\"beforeData\":null,\"headers\":{\"operation\":\"UPDATE\",\"timestamp\":\"2021-01-27T13:42:13.383\",\"streamPosition\":\"123154689132138433181312132194984313218\"}}"));
        ConsumerRecords<String, String> messages = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> message : messages) {
            assert "{\"data\":{\"a\":\"foo\",\"b\":\"bar\",\"c\":123}}".equalsIgnoreCase(message.value());
            return;
        }
    }

    @Test
    public void testDelete() throws ExecutionException, InterruptedException {
        producer.send(new ProducerRecord<>("my-cdc-input-topic",
                "{\"pk\":\"123456\",\"data\":{\"a\":\"foo\",\"b\":\"bar\",\"c\":123},\"beforeData\":null,\"headers\":{\"operation\":\"DELETE\",\"timestamp\":\"2021-01-27T13:42:13.383\",\"streamPosition\":\"123154689132138433181312132194984313218\"}}"));
        ConsumerRecords<String, String> messages = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> message : messages) {
            assert message.value() == null;
            return;
        }
    }

    @Test
    public void testNonJsonInput() throws ExecutionException, InterruptedException {
        // this message will be ignored, but the app won't crash, and process the next one
        producer.send(new ProducerRecord<>("my-cdc-input-topic", "hello"));
        producer.send(new ProducerRecord<>("my-cdc-input-topic",
                "{\"pk\":\"123456\",\"data\":{\"a\":\"foo\",\"b\":\"bar\",\"c\":123},\"beforeData\":null,\"headers\":{\"operation\":\"UPDATE\",\"timestamp\":\"2021-01-27T13:42:13.383\",\"streamPosition\":\"123154689132138433181312132194984313218\"}}"));
        ConsumerRecords<String, String> messages = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> message : messages) {
            assert "{\"data\":{\"a\":\"foo\",\"b\":\"bar\",\"c\":123}}".equalsIgnoreCase(message.value());
            return;
        }
    }

    @AfterClass
    public static void close() {
        producer.close();
        consumer.close();
    }

}
