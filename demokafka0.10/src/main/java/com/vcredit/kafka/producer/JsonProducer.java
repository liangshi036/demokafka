package com.vcredit.kafka.producer;
/*

import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


public class JsonProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop2:9092,hadoop3:9092");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 20);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 335544320);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", HashPartitioner.class.getName());

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 20; i++) {
            ProducerRecord record = new ProducerRecord<String, String>
                    ("JXL_INS", "key:" + Integer.toString(i), "value:" + Integer.toString(i));
            //producer.send(record);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.printf("Send record partition:%d, offset:%d, keysize:%d,valuesize:%d ,%n",
                            metadata.partition(), metadata.offset(), metadata.serializedKeySize(),
                            metadata.serializedValueSize());
                }

            });

//        String jsonRec = "{\"topic\": \"JXL_INS\",\n" +
//                "  \"data\": {\n" +
//                "    \"jxl_basic\": [\n" +
//                "      {\n" +
//                "        \"Basic_id\": 44795612,\n" +
//                "        \"Oper_id\": 46839947,\n" +
//                "        \"Cell_phone\": \"13931047880\",\n" +
//                "        \"Real_name\": \"陈**\",\n" +
//                "        \"Reg_time\": \"2013-09-13 16:57:26\",\n" +
//                "        \"idcard\": null,\n" +
//                "        \"Update_time\": \"2019-03-11 17:05:44\"\n" +
//                "      }\n" +
//                "    ]}}";
//        String jsonRec = "aaa";
//        ProducerRecord<String, String> rec = new ProducerRecord<String, String>("JXL_INS","aaa", "bbb");
//        producer.send(rec);

//        producer.send(rec, new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata metadata, Exception exception) {
//                System.out.printf("Send record partition:%d, offset:%d, keysize:%d,valuesize:%d ,%n",
//                        metadata.partition(), metadata.offset(), metadata.serializedKeySize(),
//                        metadata.serializedValueSize());
//            }
//
//        });
        }
    }
}
*/


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class JsonProducer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop2:9092,hadoop3:9092");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 20);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("partitioner.class", HashPartitioner.class.getName());

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        String recJson = "{\n" +
                "\"topic\":\"JXL_INS\",\n" +
                "\"data\":{\n" +
                "\"jxl_basic\":[\n" +
                "{\n" +
                "\"Basic_id\":44795612,\n" +
                "\"Oper_id\":46839947,\n" +
                "\"Cell_phone\":\"13931047880\",\n" +
                "\"Real_name\":\"薛贤巨\",\n" +
                "\"Reg_time\":\"2013-09-13 16:57:26\",\n" +
                "\"idcard\":null,\n" +
                "\"Update_time\":\"2019-03-11 17:05:44\"\n" +
                "}\n" +
                "],\n" +
                "\"jxl_calls\":[\n" +
                "{\n" +
                "\"Call_id\":0,\n" +
                "\"Oper_id\":null,\n" +
                "\"Cell_phone\":\"13931047880\",\n" +
                "\"Other_cell_phone\":\"15313159536\",\n" +
                "\"Call_place\":null,\n" +
                "\"Start_time\":\"2018-10-20 10:22:58\",\n" +
                "\"Use_time\":\"261\",\n" +
                "\"Call_type\":\"本地主叫异地\",\n" +
                "\"Init_type\":\"主叫\",\n" +
                "\"Subtotal\":0.95,\n" +
                "\"Update_time\":\"2019-03-11 17:05:44\",\n" +
                "\"Place\":\"河北邯郸\"\n" +
                "}\n" +
                "]\n" +
                "}\n" +
                "}";


        ProducerRecord<String, String> rec = new ProducerRecord<String, String>("JXL_INS",recJson);

        //producer.send(record);
        producer.send(rec, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                System.out.printf("Send record partition:%d, offset:%d, keysize:%d,valuesize:%d ,%n",
                        metadata.partition(), metadata.offset(), metadata.serializedKeySize(),
                        metadata.serializedValueSize());
            }

        });
        producer.close();
    }
}
