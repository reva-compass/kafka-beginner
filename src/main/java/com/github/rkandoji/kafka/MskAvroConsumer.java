package com.github.rkandoji.kafka;

import com.google.gson.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class MskAvroConsumer {

    public static void main(String[] args) {
        System.out.println("Hello there!");

        Gson GSON = new GsonBuilder().create();
        Logger LOG = LoggerFactory.getLogger(MskAvroConsumer.class);
        String bootstrapServer = "b-1.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092," +
                "b-2.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092," +
                "b-3.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092," +
                "b-4.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092," +
                "b-5.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092," +
                "b-6.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092 ";
        String groupId = UUID.randomUUID().toString();
        //     String topic = "data_listings_json_listings_joined_aspen_mls_rets_av_1";
        String topic = "data_listings_avro_listings_joined_aspen_mls_rets_av_1";

        // create consumer configs
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // subscribe consumer to topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                LOG.info("Key:" + record.key() + " Value:" + record.value());
                LOG.info("Partition:" + record.partition() + " Offset:" + record.offset());

                String val = record.value().toString().replace("\\", "");
                System.out.println("### val " + val);

                try {
                    JsonObject jo = new JsonParser().parse(val).getAsJsonObject();
                    System.out.println("## jo " + jo);
                    System.out.println("# " + jo.get("data_version").getAsInt());
                } catch (JsonSyntaxException e) {
                    LOG.error("Error occurred: " + e.getMessage());
                }
            }

        }
    }
}
