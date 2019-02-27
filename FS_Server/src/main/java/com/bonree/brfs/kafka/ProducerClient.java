package com.bonree.brfs.kafka;

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date: 19-2-27下午6:07
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description:
 ******************************************************************************/
public class ProducerClient {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerClient.class);

    private KafkaProducer<String,String> producer;
    private String topic;


    private ProducerClient(){
        this.topic = Configs.getConfiguration().GetConfig(KafkaConfig.CONFIG_TOPIC);
        Properties props = new Properties();
        props.put("bootstrap.servers", Configs.getConfiguration().GetConfig(KafkaConfig.CONFIG_BROKERS));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("request.required.acks", "0");
        producer = new KafkaProducer<>(props);
    }

    public static ProducerClient getInstance(){
        return Holder.client;
    }

    private static class Holder{
        private final static ProducerClient client = new ProducerClient();

    }

    public void sendMessage(String message){
        ProducerRecord<String,String> record = new ProducerRecord<>(topic,message);
        producer.send(record);
    }

//    public static void main(String[] args) {
//        ProducerClient.getInstance().sendMessage("aaaaaaaaaaaaaaaaaaaaa");
//    }

}
