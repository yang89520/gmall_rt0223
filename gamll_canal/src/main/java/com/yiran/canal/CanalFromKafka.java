package com.yiran.canal;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.yiran.gmall.Constansts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.protocol.Message;
/**
 * @program: gmall_rt0223
 * @description:
 * @author: Mr.Yang
 * @create: 2021-07-08 19:28
 **/

public class CanalFromKafka {
    protected final static Logger           logger  = LoggerFactory.getLogger(CanalFromKafka.class);

    private KafkaCanalConnector             connector;

    private static volatile boolean         running = false;

    private Thread                          thread  = null;

    private Thread.UncaughtExceptionHandler handler = (t, e) -> logger.error("parse events has an error", e);


    public static String servers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static String topic = Constansts.GMALL_DB_CANAL;
    public static Integer partition = 3;
    public static String groupId = Constansts.GMALL_DB_CANAL_GROUP;

    public CanalFromKafka(String zkServers, String topic, Integer partition, String groupId){
        connector = new KafkaCanalConnector(servers, topic, partition, groupId, null, false);
    }

    public static void main(String[] args) {
        try {
            final CanalFromKafka canalToKafka = new CanalFromKafka(servers,
                    topic,
                    partition,
                    groupId);
            logger.info("## start the kafka consumer: {}-{}", topic, groupId);
            canalToKafka.start();
            logger.info("## the canal kafka consumer is running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    logger.info("## stop the kafka consumer");
                    canalToKafka.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping kafka consumer:", e);
                } finally {
                    logger.info("## kafka consumer is down.");
                }
            }));
            while (running)
                ;
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the kafka consumer:", e);
            System.exit(0);
        }
    }

    public void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(this::process);
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private void process() {
        while (!running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                while (running) {
                    try {
                        List<Message> messages = connector.getListWithoutAck(100L, TimeUnit.MILLISECONDS); // 获取message
                        if (messages == null) {
                            continue;
                        }
                        for (Message message : messages) {
                            long batchId = message.getId();
                            int size = message.getEntries().size();
                            if (batchId == -1 || size == 0) {
                                // try {
                                // Thread.sleep(1000);
                                // } catch (InterruptedException e) {
                                // }
                            } else {
                                // printSummary(message, batchId, size);
                                // printEntry(message.getEntries());
                                logger.info(message.toString());
                            }
                        }

                        connector.ack(); // 提交确认
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        connector.unsubscribe();
        connector.disconnect();
    }
}
