package com.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

public class RedisQueueService implements PriorityQueueService {

    private final Jedis jedis;
    private Integer visibilityTimeout;
    private final Logger logger = LogManager.getLogger(RedisQueueService.class);
    private final Gson gson = new GsonBuilder().serializeNulls().create();

    public RedisQueueService() {
        this.jedis = new Jedis("apn1-pet-wombat-34614.upstash.io", 34614, true);
        this.jedis.auth("a60ea24024a240a09842e11688682b6b");

        String propFileName = "config.properties";
        Properties confInfo = new Properties();
        try (InputStream inStream = getClass().getClassLoader().getResourceAsStream(propFileName)) {
            confInfo.load(inStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.visibilityTimeout = Integer.parseInt(confInfo.getProperty("visibilityTimeout", "30"));
    }

    @Override
    public void push(String queueUrl, String msgBody, int rank) {
        Long now = now();
        PriorityMessage priorityMessage = new PriorityMessage(rank, now, new Message(msgBody));
        try {
            String serializedMessage = gson.toJson(priorityMessage);
            logger.debug(serializedMessage);
            this.jedis.zadd(queueUrl, score(priorityMessage), serializedMessage);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public Message pull(String queueUrl) {
        Long nowTime = now();
        try {
            Set<Tuple> tuples = this.jedis.zrangeWithScores(queueUrl, 0, 0);
            logger.debug(tuples.toString());
            for (Tuple tuple : tuples) {
                String deserializedMessage = tuple.getElement();
                logger.debug(deserializedMessage);
                PriorityMessage priorityMessage = gson.fromJson(deserializedMessage, PriorityMessage.class);
                if (priorityMessage != null && priorityMessage.getMessage() != null) {
                    Message msg = priorityMessage.getMessage();
                    msg.setReceiptId(UUID.randomUUID().toString());
                    msg.incrementAttempts();
                    msg.setVisibleFrom(nowTime + TimeUnit.SECONDS.toMillis(visibilityTimeout));
            
                    return new Message(msg.getBody(), msg.getReceiptId());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void delete(String queueUrl, String receiptId) {
        
    }
    
    long now() {
        return System.currentTimeMillis();
    }

    private double score(PriorityMessage priorityMessage) {
        double messageScore = (double) priorityMessage.getRank() + (double) priorityMessage.getTime() / 1e12;
        System.out.printf("Score :%f\n", messageScore);
        return messageScore;
    }
    
    public static void main(String[] args) {
        RedisQueueService redisQueueService = new RedisQueueService();
        // redisQueueService.push("abc.com", "Hello", 1);
        // redisQueueService.push("abc.com", "Hi", 2);
        // redisQueueService.push("abc.com", "Hiya", 3);

        Message message = redisQueueService.pull("abc.com");
        if (message != null) {
            System.out.println(message.getBody() + message.getReceiptId() + message.getAttempts());
        }
        
    }
}   
