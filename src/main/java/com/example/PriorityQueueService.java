package com.example;

public interface PriorityQueueService {

    public void push(String queueUrl, String msgBody, int rank);

    public Message pull(String queueUrl);

    public void delete(String queueUrl, String receiptId);

}