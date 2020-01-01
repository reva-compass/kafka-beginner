package com.github.rkandoji.kafka.crmls;

import com.github.rkandoji.kafka.consumers.MskJsonConsumer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class CrmlsListingsManager {
    public static void main(String[] args) {
        System.out.println("Hello there!");

        Gson GSON = new GsonBuilder().create();
        Logger LOG = LoggerFactory.getLogger(MskJsonConsumer.class);
        String bootstrapServer = "b-3.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092," +
                "b-2.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092," +
                "b-1.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092";
        String groupId = UUID.randomUUID().toString();
        String inTopic = "la_crmls_rets-listings-neo";

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
        consumer.subscribe(Arrays.asList(inTopic));

        // create producer
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        String outTopic = "la_crmls_rets-listings-neo-modified";
        JsonParser parser = new JsonParser();

        try {

            // poll for data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    LOG.info("Key:" + record.key() + " Value:" + record.value());
                    LOG.info("Partition:" + record.partition() + " Offset:" + record.offset());
                    String jsonStr = record.value();
                    JsonObject inputObj = parser.parse(jsonStr).getAsJsonObject();
                    JsonObject dataObj = parser.parse(inputObj.get("data").getAsString()).getAsJsonObject();
                    String listingId = dataObj.get("ListingKeyNumeric").getAsString();

                    if (dataObj.has("ListAgentKeyNumeric"))
                        inputObj.addProperty("ListAgentKeyNumeric", dataObj.get("ListAgentKeyNumeric").getAsString());
                    if (dataObj.has("BuyerAgentKeyNumeric"))
                        inputObj.addProperty("BuyerAgentKeyNumeric", dataObj.get("BuyerAgentKeyNumeric").getAsString());
                    if (dataObj.has("CoListAgentKeyNumeric"))
                        inputObj.addProperty("CoListAgentKeyNumeric", dataObj.get("CoListAgentKeyNumeric").getAsString());
                    if (dataObj.has("CoBuyerAgentKeyNumeric"))
                        inputObj.addProperty("CoBuyerAgentKeyNumeric", dataObj.get("CoBuyerAgentKeyNumeric").getAsString());
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(outTopic, listingId, inputObj.toString());
                    producer.send(producerRecord);

                }

            }

        } catch (Exception e) {
            LOG.error("Error: ", e);
        }
        producer.flush();
        //  producer.close();
    }
}
