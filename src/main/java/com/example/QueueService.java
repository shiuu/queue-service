package com.example;

public interface QueueService {

  //
  // Task 1: Define me.
  //
  // This interface should include the following methods.  You should choose appropriate
  // signatures for these methods that prioritise simplicity of implementation for the range of
  // intended implementations (in-memory, file, and SQS).  You may include additional methods if
  // you choose.
  //
  // - push
  //   pushes a message onto a queue.
  // - pull
  //   retrieves a single message from a queue.
  // - delete
  //   deletes a message from the queue that was received by pull().
  //
  public void push(String queueUrl, String messageBody);
	
	public Message pull(String queueUrl);

	public void delete(String queueUrl, String receiptId);

}
