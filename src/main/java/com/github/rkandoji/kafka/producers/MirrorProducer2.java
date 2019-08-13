package com.github.rkandoji.kafka.producers;

import com.github.rkandoji.kafka.consumers.MskJsonConsumer;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
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

// send data as "payload" with some metadata on top level

public class MirrorProducer2 {

    public static void main(String[] args) {
        System.out.println("Hello there!");

        Logger LOG = LoggerFactory.getLogger(MskJsonConsumer.class);
        String bootstrapServer = "b-1.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092," +
                "b-2.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092," +
                "b-3.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092," +
                "b-4.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092," +
                "b-5.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092," +
                "b-6.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092 ";
        String groupId = UUID.randomUUID().toString();
        String topic = "data_listings_json_listings_joined_aspen_mls_rets_av_1";

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

        // create producer
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        String propertyTopic = "poc_aspen_property";

        // poll for data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                LOG.info("Key:" + record.key() + " Value:" + record.value());
                LOG.info("Partition:" + record.partition() + " Offset:" + record.offset());
                try {
                    JsonParser parser = new JsonParser();
                    JsonObject jo = parser.parse(record.value()).getAsJsonObject();
                    System.out.println("### jo " + jo);
                    String payload = jo.get("payload").getAsString();
                    JsonObject payloadObject = parser.parse(payload).getAsJsonObject();

                    // AGENT
                    if (payloadObject.has("ActiveAgent:Agent")) {
                        String agentTopic = "poc_aspen_agent";
                        JsonArray agentsInfo = payloadObject.get("ActiveAgent:Agent").getAsJsonArray();
                        for (JsonElement agent : agentsInfo) {
                            // create a producer record and send
                            String aId = agent.getAsJsonObject().get("Agent ID").getAsString();
                            System.out.println("##v agent.getAsJsonObject().toString()) " + agent.getAsJsonObject().toString());
                            JsonObject ja = new JsonObject();
                            ja.addProperty("agent_id", aId);
                            ja.addProperty("payload", agent.getAsJsonObject().toString());
                            System.out.println("### ja " + ja);
                            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(agentTopic, aId, ja.toString());
                            producer.send(producerRecord);
                        }
                    }

                    // OPEN HOUSE
//                    if (payloadObject.has("OpenHouse:OpenHouse")) {
//                        String ohTopic = "poc_aspen_open_house";
//                        JsonArray openHouses = payloadObject.get("OpenHouse:OpenHouse").getAsJsonArray();
//                        for (JsonElement oh : openHouses) {
//                            // create a producer record and send
//                            String ohId = oh.getAsJsonObject().get("Event Unique ID").getAsString();
//                            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(ohTopic, ohId, oh.getAsJsonObject().toString());
//                            producer.send(producerRecord);
//                        }
//                    }

                    // OFFICE
//                    if (payloadObject.has("Office:Office")) {
//                        String officeTopic = "poc_aspen_office";
//                        JsonArray offices = payloadObject.get("Office:Office").getAsJsonArray();
//                        for (JsonElement office : offices) {
//                            // create a producer record and send
//                            String officeId = office.getAsJsonObject().get("Office ID").getAsString();
//                            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(officeTopic, officeId, office.getAsJsonObject().toString());
//                            producer.send(producerRecord);
//                        }
//
//                    }

                    // create property root
                    JsonObject rootObject = createRootJson(parser, payload);

                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(propertyTopic, rootObject.get("internal_listing_id").getAsString(), rootObject.toString());
                    producer.send(producerRecord);

                    // flush and close (since send is async, you must do it see data...otherwise applciation will close before send is executed
                    producer.flush();
                    //  producer.close();

                } catch (Exception e) {
                    LOG.error("Error: ", e);
                }
            }

        }

    }

    private static JsonObject createRootJson(JsonParser parser, String payload) {

        JsonObject jo = parser.parse(payload).getAsJsonObject();
        JsonObject out = new JsonObject();

        // add root level items
        if (jo.has("Internal Listing ID")) {
            String internalListingId = jo.get("Internal Listing ID").getAsString();
            out.addProperty("internal_listing_id", internalListingId);
        }
        if (jo.has("Agent ID")) {
            String agentId = jo.get("Agent ID").getAsString();
            out.addProperty("agent_id", agentId);
        }
        if (jo.has("CoList Agent ID")) {
            String coAgentId = jo.get("CoList Agent ID").getAsString();
            out.addProperty("colist_agent_id", coAgentId);
        }
        if (jo.has("Selling Agent ID")) {
            String sellingAgentId = jo.get("Selling Agent ID").getAsString();
            out.addProperty("selling_agent_id", sellingAgentId);
        }
        if (jo.has("CoSelling Agent ID")) {
            String coSellingAgentId = jo.get("CoSelling Agent ID").getAsString();
            out.addProperty("coselling_agent_id", coSellingAgentId);
        }

        // remove joined items
        if (jo.has("ActiveAgent:Agent")) {
            jo.remove("ActiveAgent:Agent");
        }
        if (jo.has("OpenHouse:OpenHouse")) {
            jo.remove("OpenHouse:OpenHouse");
        }
        if (jo.has("Office:Office")) {
            jo.remove("Office:Office");
        }

        out.addProperty("payload", jo.toString());
        return out;
    }
}
