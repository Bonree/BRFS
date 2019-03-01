package com.bonree.brfs.kafka;

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.KafkaConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date: 19-2-27下午6:07
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description:
 ******************************************************************************/
public class ProducerClient implements KafkaClient {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerClient.class);

    private KafkaProducer<String, String> producer;
    private String topic;
    private int queueSize;

    private boolean kafkaSwitch;

    private BlockingQueue<String> msgQueue;

    private Thread sendThread;


    private ProducerClient() {
        this.topic = //"brfs_merit";
                Configs.getConfiguration().GetConfig(KafkaConfig.CONFIG_TOPIC);
        Properties props = new Properties();
        props.put("bootstrap.servers", /*"192.168.107.13:9092"*/Configs.getConfiguration().GetConfig(KafkaConfig
                .CONFIG_BROKERS)
        );
        props.put("linger.ms", "5");
        props.put("max.request.size", "10485760");
        props.put("compression.type", "snappy");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("request.required.acks", "1");
        props.put("batch.size", "16384");
        props.put("buffer.memory", "104857600");
        props.put("request.timeout.ms", "90000");
        producer = new KafkaProducer<>(props);

        this.queueSize = //100000;
                Configs.getConfiguration().GetConfig(KafkaConfig.CONFIG_QUEUE_SIZE);


        msgQueue = new ArrayBlockingQueue(queueSize);

        this.kafkaSwitch = //true;
                Configs.getConfiguration().GetConfig(KafkaConfig.CONFIG_KAFKA_SWITCH);

        sendThread = new Thread(new Sender(), "kafka-client");
        sendThread.setDaemon(true);
        sendThread.start();
    }

    public static ProducerClient getInstance() {
        return Holder.client;
    }

    @Override
    public void close() throws IOException {
        sendThread.interrupt();
        producer.close();
    }

    private static class Holder {
        private final static ProducerClient client = new ProducerClient();

    }

    @Override
    public boolean sendMessage(String message) {
        if (kafkaSwitch) {

            boolean sucessful = msgQueue.offer(message);
            if (!sucessful) {
                LOG.warn("kafka merit queue is full.");
                return false;
            }

        }
        return true;
    }

    private class Sender implements Runnable {
        @Override
        public void run() {
            String msg = null;
            try {
                while (true) {
                    msg = msgQueue.poll(1, TimeUnit.SECONDS);
                    if (StringUtils.isNotEmpty(msg)) {
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
                        producer.send(record);
                    }
                }
            } catch (InterruptedException e) {
                //..ignore
            } catch (Exception e) {
                LOG.error("save kafka merit : {} is error!", msg);
            }
        }
    }

}
