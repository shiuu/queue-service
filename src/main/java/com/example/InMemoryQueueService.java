package com.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class InMemoryQueueService implements QueueService {
  private final Map<String, Queue<Message>> queues;

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
  public void push(String queueUrl, String msgBody) {
    Queue<Message> queue = queues.get(queueUrl);
    if (queue == null) {
      queue = new ConcurrentLinkedQueue<>();
      queues.put(queueUrl, queue);
    }
    queue.add(new Message(msgBody));
  }

  @Override
  public Message pull(String queueUrl) {
    Queue<Message> queue = queues.get(queueUrl);
    if (queue == null) {
      return null;
    }

    long nowTime = now();
    Optional<Message> msgOpt = queue.stream().filter(m -> m.isVisibleAt(nowTime)).findFirst();
    if (msgOpt.isEmpty()) {
      return null;
    } else {
      Message msg = msgOpt.get();
      msg.setReceiptId(UUID.randomUUID().toString());
      msg.incrementAttempts();
      msg.setVisibleFrom(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(visibilityTimeout));

      return new Message(msg.getBody(), msg.getReceiptId());
    }
  }

  @Override
  public void delete(String queueUrl, String receiptId) {
    Queue<Message> queue = queues.get(queueUrl);
    if (queue != null) {
      long nowTime = now();

      for (Iterator<Message> it = queue.iterator(); it.hasNext(); ) {
        Message msg = it.next();
        if (!msg.isVisibleAt(nowTime) && msg.getReceiptId().equals(receiptId)) {
          it.remove();
          break;
        }
      }
    }
  }

  long now() {
    return System.currentTimeMillis();
  }
}
