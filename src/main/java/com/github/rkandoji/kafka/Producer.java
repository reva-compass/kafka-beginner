package com.github.rkandoji.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * This is a producer class writing data to local Kafka
 * 
 * start the consumer using below command:
 * [rkandoji] ~/Downloads/kafka_2.12-2.3.0 $ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group my-first-application
 */
public class Producer {

    public static void main(String[] args) {
        System.out.println("Hello yay Reva!!");

        String bootstrapServer = "localhost:9092";
        String topic = "first_topic";

        // create producer properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "hello from kafka");
        // send data
        producer.send(record);

        // flush and close (since send is async, you must do it see data...otherwise applciation will close before send is executed
        producer.flush();
        producer.close();
    }
}
