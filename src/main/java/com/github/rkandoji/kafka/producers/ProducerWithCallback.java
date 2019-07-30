package com.github.rkandoji.kafka.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithCallback {
    public static void main(String[] args) {

        final Logger LOG = LoggerFactory.getLogger(ProducerWithCallback.class);

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

        for (int i = 0; i <= 10; i++) {

            String key = "id_" + i;
            String val = "hello from kafka " + i;
            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, val);
            // send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null) {
                        LOG.info("Record Metadata: " +
                                "\n Topic: " + recordMetadata.topic() +
                                "\n Partition: " + recordMetadata.partition() +
                                "\n Offset: " + recordMetadata.offset() +
                                "\n TimeStamp: " + recordMetadata.timestamp());
                    } else {
                        // Exception scenario
                        LOG.error("Error while producing.", e);
                    }
                }
            });
        }
        // flush and close (since send is async, you must do it see data...otherwise applciation will close before send is executed
        producer.flush();
        producer.close();
    }
}
