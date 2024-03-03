package com.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class InMemoryQueueService implements PriorityQueueService  {
  private final Map<String, PriorityBlockingQueue<PriorityMessage>> queues;

  private long visibilityTimeout;

  InMemoryQueueService() {
    this.queues = new ConcurrentHashMap<>();
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
    // priority --> numerically lowest rank, lowest time, message
    PriorityBlockingQueue<PriorityMessage> queue = queues.get(queueUrl);
    if (queue == null) {
      queue = new PriorityBlockingQueue<PriorityMessage>();
      queues.put(queueUrl, queue);
    }
    Long now = now();
    PriorityMessage priorityMessage = new PriorityMessage(rank,now, new Message(msgBody));
    queue.add(priorityMessage);
  }

  @Override
  public Message pull(String queueUrl) {
    PriorityBlockingQueue<PriorityMessage> queue = queues.get(queueUrl);
    if (queue == null) {
      return null;
    }
    long nowTime = now();

    while (!queue.isEmpty()) {
      PriorityMessage priorityMessage = queue.poll();
      if (priorityMessage == null) {
        return null;
      } else {
        Message msg = priorityMessage.getMesssage();
        msg.setReceiptId(UUID.randomUUID().toString());
        msg.incrementAttempts();
        msg.setVisibleFrom(nowTime + TimeUnit.SECONDS.toMillis(visibilityTimeout));
  
        return new Message(msg.getBody(), msg.getReceiptId());
      }
    }
    return null;
  }

  @Override
  public void delete(String queueUrl, String receiptId) {
    PriorityBlockingQueue<PriorityMessage> queue = queues.get(queueUrl);
    if (queue != null) {
      long nowTime = now();
      for (PriorityMessage priorityMessage : queue) {
        Message message = priorityMessage.getMesssage();
        if (!message.isVisibleAt(nowTime) && message.getReceiptId().equals(receiptId)) {
          queue.remove(priorityMessage);
          break;
        }
      }
    }
  }

  long now() {
    return System.currentTimeMillis();
  }
}
