package com.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class RedisQueueServiceTest {

    private RedisQueueService redisQueueService = new RedisQueueService();
    public static Logger logger = LogManager.getLogger();

    @Test
    public void testDelete() {

    }

    @Test
    public void testPull() {
        redisQueueService.push("test2.com", "Pull testing",2);
        Message message = redisQueueService.pull("test2.com");
        assertEquals("Pull testing", message.getBody());
        assertTrue(message.getReceiptId() != null && message.getReceiptId().length() > 0);
    }

    @Test
    public void testPush() {
        redisQueueService.push("test.com", "Push testing",1);
        Message message = redisQueueService.pull("test.com");
        
        assertEquals("Push testing", message.getBody());
        assertTrue(message.getReceiptId() != null && message.getReceiptId().length() > 0);
    }
}
