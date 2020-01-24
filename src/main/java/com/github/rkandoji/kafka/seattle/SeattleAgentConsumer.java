package com.github.rkandoji.kafka.seattle;

import com.github.rkandoji.kafka.consumers.MskJsonConsumer;
import com.github.rkandoji.kafka.utils.AvroUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class SeattleAgentConsumer {
    public static void main(String[] args) {
        System.out.println("Hello there!");

        Gson GSON = new GsonBuilder().create();
        Logger LOG = LoggerFactory.getLogger(MskJsonConsumer.class);
        String bootstrapServer = "b-3.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092," +
                "b-2.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092," +
                "b-1.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092";
        String groupId = UUID.randomUUID().toString();
        String inTopic = "data_listings_mirrored_seattle_nwmls_agent";

        // create consumer configs
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);

        // subscribe consumer to topic
        consumer.subscribe(Arrays.asList(inTopic));

        JsonParser parser = new JsonParser();

        try {

            // poll for data
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, byte[]> record : records) {
                    LOG.info("Key:" + record.key() + " Value:" + record.value());
                    LOG.info("Partition:" + record.partition() + " Offset:" + record.offset());
                    GenericRecord message = AvroUtils.deSerializeMessageee(record.value());
                    System.out.println("### message " + message);
                    ByteBuffer bb = (ByteBuffer) message.get("payload");
                    if(bb.hasArray()) {
                        String converted = new String(bb.array(), "UTF-8");
                        System.out.println("### converted " + converted);
                        JsonObject jo = parser.parse(converted).getAsJsonObject();
                        System.out.println("### jo " + jo.get("FirstName"));
                    }}
            }

        } catch (Exception e) {
            LOG.error("Error: ", e);
        }
    }
}
