package com.example;

import java.util.List;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;

public class SqsQueueService implements QueueService {
  //
  // Task 4: Optionally implement parts of me.
  //
  // This file is a placeholder for an AWS-backed implementation of QueueService.  It is included
  // primarily so you can quickly assess your choices for method signatures in QueueService in
  // terms of how well they map to the implementation intended for a production environment.
  //
	
	private AmazonSQS sqs;
	
  public SqsQueueService(AmazonSQSClient sqsClient) {
  	this.sqs = sqsClient;
  }
  
  @Override
	public void push(String queueUrl, String messageBody) {
  	sqs.sendMessage(queueUrl, messageBody);
	}
	
	@Override
	public com.example.Message pull(String queueUrl){
		List<com.amazonaws.services.sqs.model.Message> messages = sqs.receiveMessage(queueUrl).getMessages();
		
		if(messages == null || messages.isEmpty()) {
			return null;
		}
		
		com.amazonaws.services.sqs.model.Message sqsMsg = messages.get(0);
		
		return new com.example.Message(sqsMsg.getBody(), sqsMsg.getReceiptHandle());
	}
	
	@Override
	public void delete(String queueUrl, String receiptId) {
		sqs.deleteMessage(queueUrl, receiptId);
	}
}
