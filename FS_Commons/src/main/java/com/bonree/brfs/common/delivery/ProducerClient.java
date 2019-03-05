package com.bonree.brfs.common.delivery;

import com.bonree.bigdata.zeus.delivery.handler.Callback;
import com.bonree.bigdata.zeus.delivery.handler.Delivery;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.KafkaConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
public class ProducerClient implements Deliver {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerClient.class);

    private String tableWriter;
    private String tableReader;
    private int queueSize;

    private boolean deliverSwitch;

    private BlockingQueue<DataTuple> msgQueue;

    private Thread sendThread;

    private Delivery delivery;

    private String dataSource;

    private String topic;

    private String metaUrl;

    private static final String USER_NAME = "brfs";
    private static final String TOKEN = "brfs";

    private ProducerClient() {

        this.queueSize = //100000;
                Configs.getConfiguration().GetConfig(KafkaConfig.CONFIG_QUEUE_SIZE);
        msgQueue = new ArrayBlockingQueue(queueSize);
        this.deliverSwitch = //true;
                Configs.getConfiguration().GetConfig(KafkaConfig.CONFIG_DELIVER_SWITCH);

        this.dataSource = Configs.getConfiguration().GetConfig(KafkaConfig.CONFIG_DATA_SOURCE);
        this.topic = Configs.getConfiguration().GetConfig(KafkaConfig.CONFIG_TOPIC);
        this.metaUrl = Configs.getConfiguration().GetConfig(KafkaConfig.CONFIG_META_URL);
        this.tableReader = Configs.getConfiguration().GetConfig(KafkaConfig.CONFIG_READER_TABLE);
        this.tableWriter = Configs.getConfiguration().GetConfig(KafkaConfig.CONFIG_WRITER_TABLE);

        try {
            build();
        } catch (Exception e) {
            LOG.error("deliver client build failed!", e);
        }

        sendThread = new Thread(new Sender(), "kafka-client");
        sendThread.setDaemon(true);
        sendThread.start();

    }


    public static Deliver getInstance() {
        return Holder.client;
    }

    @Override
    public void close() throws IOException {
        sendThread.interrupt();
    }

    private static class Holder {
        private final static ProducerClient client = new ProducerClient();

    }

    private boolean sendMessage(String type, Map<String, Object> data) {
        if(delivery == null){
            try {
                build();
            } catch (Exception e) {
                LOG.error("deliver client build failed!", e);
            }
        }

        if (deliverSwitch) {
            DataTuple dt = new DataTuple(type, data);
            boolean successful = msgQueue.offer(dt);
            if (!successful) {
                LOG.warn("queue is full.");
                return false;
            }
            
            LOG.info("send message {} : {}", type, data);
        }
        return true;
    }

    @Override
    public boolean sendWriterMetric(Map<String, Object> data) {
        return sendMessage(tableWriter, data);
    }

    @Override
    public boolean sendReaderMetric(Map<String, Object> data) {
        return sendMessage(tableReader, data);
    }

    private class Sender implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    DataTuple dt = msgQueue.poll(1, TimeUnit.SECONDS);
                    if (null != dt && null != dt._2()) {
                        delivery.add(dt._1(), dt._2(), new Callback() {
                            @Override
                            public void onSuccess(int i) {

                            }

                            @Override
                            public void onFail(Exception e) {
                                LOG.warn("output to zeus filed, data:{}", dt, e);
                            }
                        });
                    }
                }
            } catch (InterruptedException e) {
                //..ignore
            } catch (Exception e) {
                LOG.error("save kafka metric is error!", e);
            }
        }
    }

    private void build() throws Exception {
        LOG.info("build deliver client:{}",toString());
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", /*"192.168.107.13:9092"*/Configs.getConfiguration().GetConfig(KafkaConfig
                .CONFIG_BROKERS)
        );
        props.put("linger.ms", "5");
        props.put("max.request.size", "10485760");
        props.put("compression.type", "snappy");
        props.put("key.serializer", ByteArraySerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        props.put("request.required.acks", "1");
        props.put("batch.size", "16384");
        props.put("buffer.memory", "104857600");
        props.put("request.timeout.ms", "90000");

        delivery =
                new Delivery.Builder().setMetadataURL(metaUrl)
                        .setUsername(USER_NAME)
                        .setToken(TOKEN)
                        .setDataSource(dataSource)
                        .setProducerParams(props)
                        .setTopic(topic)
                        .setVersion(Delivery.KafkaVersion.VERSION_10).build();
        LOG.info("deliver client:{}",toString());
    }

    @Override
    public String toString() {
        return "ProducerClient{" +
                "tableWriter='" + tableWriter + '\'' +
                ", tableReader='" + tableReader + '\'' +
                ", queueSize=" + queueSize +
                ", deliverSwitch=" + deliverSwitch +
                ", msgQueue=" + msgQueue +
                ", sendThread=" + sendThread +
                ", delivery=" + delivery +
                ", dataSource='" + dataSource + '\'' +
                ", topic='" + topic + '\'' +
                ", metaUrl='" + metaUrl + '\'' +
                '}';
    }

    private static class DataTuple {
        private final String type;

        private final Map<String, Object> data;

        public DataTuple(String type, Map<String, Object> data) {
            this.type = type;
            this.data = data;
        }

        public String _1() {
            return type;
        }

        public Map<String, Object> _2() {
            return data;
        }

        @Override
        public String toString() {
            return "DataTuple{" +
                    "type=" + type +
                    ", data=" + data +
                    '}';
        }
    }


}
