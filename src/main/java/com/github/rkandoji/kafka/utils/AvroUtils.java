package com.github.rkandoji.kafka.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class AvroUtils {

    public static GenericRecord deSerializeMessageee(byte[] data) throws IOException {
        GenericRecord envelope = deSerializeEnvelope(data);
        ByteBuffer bb = (ByteBuffer) envelope.get("message");
        GenericRecord message = null;
        if (bb.hasArray()) {
            message = AvroUtils.deSerializePipelineMessage(bb.array());
        }
        return message;
    }

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
        return result;
    }

}
