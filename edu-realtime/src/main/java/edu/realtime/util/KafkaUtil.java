package edu.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

public class KafkaUtil {

    public static String BOOTSTRAP_SERVERS ="hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topicName,String groupID){
        //创建配置对象
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>(
                topicName,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }
                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        if (record == null || record.value() == null){
                            return "";
                        }
                        return new String(record.value());
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                },
                properties
        );
        return flinkKafkaConsumer;
    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String topicName){
        //创建配置对象
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);

        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>(
                topicName,
                new SimpleStringSchema(),
                properties
        );

        return flinkKafkaProducer;
    }

    public static String getKafkaDDL(String topicName,String groupID) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+ topicName +"',\n" +
                "  'properties.bootstrap.servers' = '"+ BOOTSTRAP_SERVERS +"',\n" +
                "  'properties.group.id' = '"+ groupID +"',\n" +
//                "  'scan.startup.mode' = 'group-offsets',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" + //从主题中的最早开始消费，省去重新造数据
                "  'format' = 'json'\n" +
                ")";
    }

    public static String getKafkaSinkDDL(String topicName) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+ topicName +"',\n" +
                "  'properties.bootstrap.servers' = '"+ BOOTSTRAP_SERVERS +"',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }

    public static String getUpsertKafkaSinkDDL(String topicName) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+ topicName +"',\n" +
                "  'properties.bootstrap.servers' = '"+ BOOTSTRAP_SERVERS +"',\n" +
                "  'key.format' = 'json' ,\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }
}
