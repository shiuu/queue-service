package com.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class InMemoryQueueTest {
	private QueueService qs;
	private String queueUrl = "https://sqs.ap-1.amazonaws.com/007/MyQueue";
	
	@Before
	public void setup() {
		qs = new InMemoryQueueService();
	}
	
	
	@Test
	public void testSendMessage(){
		qs.push(queueUrl, "Good message!");
		Message msg = qs.pull(queueUrl);

		assertNotNull(msg);
		assertEquals("Good message!", msg.getBody());
	}
	
	@Test
	public void testPullMessage(){
		String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null }";
		
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
		String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null }";
		
		qs.push(queueUrl, msgBody);
		Message msg = qs.pull(queueUrl);

		qs.delete(queueUrl, msg.getReceiptId());
		msg = qs.pull(queueUrl);
		
		assertNull(msg);
	}
	
	@Test
	public void testFIFO3Msgs(){
		String [] msgStrs = {"TEst msg 1", "test msg 2",
				"{\n" + 								// test with multi-line message.
				"    \"name\":\"John\",\n" + 
				"    \"age\":30,\n" + 
				"    \"cars\": {\n" + 
				"        \"car1\":\"Ford\",\n" + 
				"        \"car2\":\"BMW\",\n" + 
				"        \"car3\":\"Fiat\"\n" + 
				"    }\n" + 
				" }"};
		qs.push(queueUrl, msgStrs[0]);
		qs.push(queueUrl, msgStrs[1]);
		qs.push(queueUrl, msgStrs[2]);
		Message msg1 = qs.pull(queueUrl);
		Message msg2 = qs.pull(queueUrl);
		Message msg3 = qs.pull(queueUrl);
		
		org.junit.Assert.assertTrue(msgStrs[0] == msg1.getBody()
				&& msgStrs[1] == msg2.getBody() && msgStrs[2] == msg3.getBody());
	}
	
	@Test
	public void testAckTimeout(){
		InMemoryQueueService queueService = new InMemoryQueueService() {
			long now() {
				return System.currentTimeMillis() + 1000 * 30 + 1;
			}
		};
		
		queueService.push(queueUrl, "Message A.");
		queueService.pull(queueUrl);
		Message msg = queueService.pull(queueUrl);
		assertTrue(msg != null && msg.getBody() == "Message A.");
	}
}
