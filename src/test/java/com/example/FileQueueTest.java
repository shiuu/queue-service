package com.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class FileQueueTest {
	private FileQueueService qs = new FileQueueService();
	private String queueUrl = "https://sqs.ap-1.amazonaws.com/007/MyQueue";
	
	@Before
	public void setup() {
		qs.purgeQueue(queueUrl);
	}
	
	@Test
	public void testSendMessage(){
		qs.push(queueUrl, "Good message!");
		Message msg = qs.pull(queueUrl);

		assertTrue(msg != null && msg.getBody().equals("Good message!"));
	}
	
	@Test
	public void testPullMessage(){
		String msgBody = "{\"name\":\"John\",\"age\":30,\"cars\": {\"car1\":\"Ford\",\"car2\":\"BMW\"}}";
	 	
		qs.push(queueUrl, msgBody);
		Message msg = qs.pull(queueUrl);

		assertEquals(msgBody, msg.getBody());
		assertTrue(msg.getReceiptId() != null && msg.getReceiptId().length() > 0);
	}
	
	@Test
	public void testPullEmptyQueue(){
		Message msg = qs.pull(queueUrl);
		assertNull(msg);
	}
	
	@Test
	public void testDoublePull(){
		qs.push(queueUrl, "Message A.");
		qs.pull(queueUrl);
		Message msg = qs.pull(queueUrl);
		assertNull(msg);
	}
	
	@Test
	public void testDeleteMessage(){
		String msgBody = "Message A.";
		
		qs.push(queueUrl, msgBody);
		Message msg = qs.pull(queueUrl);

		qs.delete(queueUrl, msg.getReceiptId());
		msg = qs.pull(queueUrl);
		
		assertNull(msg);
	}
	
	@Test
	public void testFIFO3Msgs(){
		String [] msgStrs = {"TEst msg 1", "test msg 2", "Test Message 3."};
		qs.push(queueUrl, msgStrs[0]);
		qs.push(queueUrl, msgStrs[1]);
		qs.push(queueUrl, msgStrs[2]);
		Message msg1 = qs.pull(queueUrl);
		Message msg2 = qs.pull(queueUrl);
		Message msg3 = qs.pull(queueUrl);
		
		org.junit.Assert.assertTrue(msg1.getBody().equals(msgStrs[0])
				&& msg2.getBody().equals(msgStrs[1]) && msg3.getBody().equals(msgStrs[2]));
	}
	
	/**
	 * Test delete/acknowledge timeout.
	 */
	@Test
	public void testAckTimeout(){
		FileQueueService queueService = new FileQueueService();

		queueService.push(queueUrl, "Message A.");
		queueService.pull(queueUrl);
		queueService.setTimeSupplier(() -> System.currentTimeMillis() + 1000 * 30 + 1);
		
		Message msg = queueService.pull(queueUrl);
		assertTrue(msg != null && msg.getBody().equals("Message A."));
	}
}
