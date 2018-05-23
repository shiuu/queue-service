package com.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class InMemoryQueueService implements QueueService {
	private final Map<String, Queue<Message>> queues;
	
	private long visibilityTimeout;
	
	InMemoryQueueService() {
		this.queues = new HashMap<String, Queue<Message>>();
		
		Properties confInfo = new Properties();

		try (InputStream inStream = Object.class.getResourceAsStream("/config.properties")) {
  		confInfo.load(inStream);
  	} catch (IOException e) {
  		e.printStackTrace();
  	}
  	
  	this.visibilityTimeout = Integer.parseInt(confInfo.getProperty("visibilityTimeout", "30"));
	}
	
	@Override
	public synchronized void push(String queueUrl, String msgBody) {
		Queue<Message> queue = queues.get(queueUrl);
		if (queue == null) {
			queue = new LinkedList<Message>();
			queues.put(queueUrl, queue);
		}
		queue.add(new Message(msgBody));
	}
	
	@Override
	public synchronized Message pull(String queueUrl){
		Queue<Message> queue = queues.get(queueUrl);
		if (queue == null) {
			return null;
		}
		
		long nowTime = now();
		Message msg = queue.stream()
				.filter(m -> m.isVisibleAt(nowTime))
				.findFirst()
				.orElse(null);
		if (msg == null) {
			return null; 
		}
		msg.setReceiptId(UUID.randomUUID().toString());
		msg.incrementAttempts();
		msg.setVisibleFrom(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(visibilityTimeout));
		
		return new Message(msg.getBody(), msg.getReceiptId());
	}
	
	@Override
	public synchronized void delete(String queueUrl, String receiptId) {
		Queue<Message> queue = queues.get(queueUrl);
		if (queue != null) {
			long nowTime = now();
			
			Message msg = queue.stream()
					.filter(m -> !m.isVisibleAt(nowTime) && m.getReceiptId().equals(receiptId))
					.findFirst().orElse(null);
			if (msg != null) {
				queue.remove(msg);
			}
		}
	}
	
	long now() {
		return System.currentTimeMillis();
	}
}
