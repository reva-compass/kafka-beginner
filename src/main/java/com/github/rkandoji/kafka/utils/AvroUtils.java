package com.github.rkandoji.kafka.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.File;
import java.io.IOException;

public class AvroUtils {
//
//    public static void deSerialize(byte[] data) throws IOException {
//
//        // out.toByteArray() = data
//        File schemaFile = new File("/Users/rkandoji/Documents/GitProjects/kafka-beginner/src/main/resources/kafka_envelope.avsc");
//        Schema schema = new Schema.Parser().parse(schemaFile);
//        GenericRecord datum = new GenericData.Record(schema);
//
//        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
//        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
//        GenericRecord result = reader.read(null, decoder);
//        System.out.println("### result " + result);
//    }

    public static GenericRecord deSerializeEnvelope(byte[] data) throws IOException {
        File schemaFile = new File("/Users/rkandoji/Documents/GitProjects/kafka-beginner/src/main/resources/kafka_envelope.avsc");
        Schema schema = new Schema.Parser().parse(schemaFile);
        return deSerialize(data, schema);
    }

    public static GenericRecord deSerializePipelineMessage(byte[] data) throws IOException {
        File schemaFile = new File("/Users/rkandoji/Documents/GitProjects/kafka-beginner/src/main/resources/pipeline_message_v1.avsc");
        Schema schema = new Schema.Parser().parse(schemaFile);
        return deSerialize(data, schema);
    }

    public static GenericRecord deSerialize(byte[] data, Schema schema) throws IOException {
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        GenericRecord result = reader.read(null, decoder);
//        System.out.println("### result " + result);
//        System.out.println("### schema_id " +  result.get("schema_id"));
//        System.out.println("### schema_name " +  result.get("schema_name"));
//        System.out.println("### message " +  result.get("message"));
        return result;
    }

}
