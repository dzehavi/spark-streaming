package org.dz.homework;

import com.google.common.base.Strings;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

public class JavaSparkApp {
    public static void main(String[] args) throws InterruptedException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        // create the JavaStreamingContext
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.setAppName("JavaSparkApp");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        // Subscribe to incoming topic.
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", KafkaJsonDeserializer.class);
        kafkaParams.put("serializedClass", InputMessage.class);
        kafkaParams.put("group.id", "group_test2");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // Subscribe to kafka. The calculation and output are inside the new message callback
        Collection<String> topics = Arrays.asList("my-cdc-input-topic");
        JavaInputDStream<ConsumerRecord<String, InputMessage>> inputMessages =
                KafkaUtils.createDirectStream(streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, InputMessage>Subscribe(topics, kafkaParams));
        JavaDStream<InputMessage> data = inputMessages
                .map(v -> {
                        InputMessage inputMessage = v.value();
                        process(inputMessage);
                        return v.value();
                    });

        data.print(); // needed for DStream registration.
        // this is the good old loop to keep us alive
        streamingContext.start();
        streamingContext.awaitTermination();

    }

    /**
     * process messages that are received over kafka
     * @param inputMessage the message to process
     */
    private static void process(InputMessage inputMessage) {
        String transaction = inputMessage.getHeaders().get("operation");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        // create a producer for output messages
        Producer<String, OutputMessage> kafkaProducer =
                new KafkaProducer<>(
                        props,
                        new StringSerializer(),
                        new KafkaJsonSerializer()
                );

        // send the date if the transaction is "UPDATE" or "INSERT"
        if (!Strings.isNullOrEmpty(transaction)) {
            if (transaction.equalsIgnoreCase("INSERT") || transaction.equalsIgnoreCase("UPDATE")) {
                // send the data field
                OutputMessage outputMessage = new OutputMessage();
                outputMessage.setData(inputMessage.getData());
                kafkaProducer.send(new ProducerRecord("my-cdc-output-topic", String.valueOf(System.currentTimeMillis()), outputMessage));
            }
            else if (transaction.equalsIgnoreCase("DELETE")) {
                // send a tombstone record
                kafkaProducer.send(new ProducerRecord("my-cdc-output-topic", String.valueOf(System.currentTimeMillis()), null));
            }
            else {
                LogManager.getLogger(JavaSparkApp.class).warn("Unknown transaction: " + transaction);
            }
        }
        else {
            LogManager.getLogger(JavaSparkApp.class).warn("Null transaction!");
        }
    }
}