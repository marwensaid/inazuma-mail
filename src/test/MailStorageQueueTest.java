package test;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;

import mail.MailUser;
import net.spy.memcached.internal.OperationFuture;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import queue.MailStorageQueue;
import queue.SerializedMail;

import com.couchbase.client.CouchbaseClient;
import com.google.gson.Gson;

public class MailStorageQueueTest
{
	private final static int ANY_SENDER = 10;
	private final static int ANY_RECEIVER_1 = 20;
	private final static int ANY_RECEIVER_2 = 30;
	
	private final static String ANY_SUBJECT_TEXT = "Subject text";
	private final static String ANY_BODY_TEXT = "Body text";

	Gson gson;
	CouchbaseClient client;
	
	MailStorageQueue mailStorageQueue;
	
	MailUser mail1;
	SerializedMail mailSerialized1;
	String mailDocumentKey1;
	
	MailUser mail2;
	SerializedMail mailSerialized2;
	String mailDocumentKey2;

	MailUser mail3;
	SerializedMail mailSerialized3;
	String mailDocumentKey3;

	String receiver1LookupDocumentKey;
	String receiver2LookupDocumentKey;
	
	OperationFuture<Boolean> futureTrue;
	OperationFuture<Boolean> futureFalse;

	@BeforeMethod
	@SuppressWarnings("unchecked")
	public void setUp() throws InterruptedException, ExecutionException
	{
		gson = new Gson();
		client = mock(CouchbaseClient.class);
		mailStorageQueue = new MailStorageQueue(client, 1, 1);
		
		mail1 = new MailUser(ANY_SENDER, ANY_RECEIVER_1, ANY_SUBJECT_TEXT, ANY_BODY_TEXT);
		mailSerialized1 = new SerializedMail(mail1.getReceiverID(), mail1.getCreated(), mail1.getKey(), gson.toJson(mail1));
		mailDocumentKey1 = "mail_" + mailSerialized1.getKey();

		mail2 = new MailUser(ANY_SENDER, ANY_RECEIVER_1, ANY_SUBJECT_TEXT, ANY_BODY_TEXT);
		mailSerialized2 = new SerializedMail(mail2.getReceiverID(), mail2.getCreated(), mail2.getKey(), gson.toJson(mail2));
		mailDocumentKey2 = "mail_" + mailSerialized2.getKey();

		mail3 = new MailUser(ANY_SENDER, ANY_RECEIVER_2, ANY_SUBJECT_TEXT, ANY_BODY_TEXT);
		mailSerialized3 = new SerializedMail(mail3.getReceiverID(), mail3.getCreated(), mail3.getKey(), gson.toJson(mail3));
		mailDocumentKey3 = "mail_" + mailSerialized3.getKey();

		receiver1LookupDocumentKey = "receiver_" + ANY_RECEIVER_1;
		receiver2LookupDocumentKey = "receiver_" + ANY_RECEIVER_2;
		
        futureTrue = mock(OperationFuture.class);
        when(futureTrue.get()).thenReturn(true);
        futureFalse = mock(OperationFuture.class);
        when(futureFalse.get()).thenReturn(false);
	}

	@Test
	public void withoutErrors()
	{
		when(client.add(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), isA(String.class))).thenReturn(futureTrue);
		
		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();
		
		verify(client).add(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client).set(eq(receiver1LookupDocumentKey), eq(0), isA(String.class));
		verifyZeroInteractions(client);
	}
	
	@Test
	public void firstPersistMailFails()
	{
		when(client.add(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenReturn(futureFalse).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), isA(String.class))).thenReturn(futureTrue);
		
		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client, times(2)).add(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client).set(eq(receiver1LookupDocumentKey), eq(0), isA(String.class));
		verifyZeroInteractions(client);
	}
	
	@Test
	public void firstPersistLookupFails()
	{
		when(client.add(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), isA(String.class))).thenReturn(futureFalse).thenReturn(futureTrue);
		
		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client).add(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client, times(2)).set(eq(receiver1LookupDocumentKey), eq(0), isA(String.class));
		verifyZeroInteractions(client);
	}
	
	@Test
	public void secondPersistLookupFails()
	{
		when(client.add(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), isA(String.class))).thenReturn(futureFalse).thenReturn(futureFalse).thenReturn(futureTrue);
		
		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client).add(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client, times(3)).set(eq(receiver1LookupDocumentKey), eq(0), isA(String.class));
		verifyZeroInteractions(client);
	}
	
	@Test
	public void firstPersistLookupFailsWithSecondMailOnQueueWithSameReceiver()
	{
		when(client.add(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenReturn(futureTrue);
		when(client.add(mailDocumentKey2, 0, mailSerialized2.getDocument())).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), isA(String.class))).thenReturn(futureFalse).thenReturn(futureTrue);
		
		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.addMail(mailSerialized2);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client).add(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client).add(mailDocumentKey2, 0, mailSerialized2.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client, times(2)).set(eq(receiver1LookupDocumentKey), eq(0), isA(String.class));
		verifyZeroInteractions(client);
	}
	
	@Test
	public void firstPersistLookupFailsWithSecondMailOnQueueWithDifferentReceiver()
	{
		when(client.add(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenReturn(futureTrue);
		when(client.add(mailDocumentKey3, 0, mailSerialized3.getDocument())).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(null);
		when(client.get(receiver2LookupDocumentKey)).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), isA(String.class))).thenReturn(futureFalse).thenReturn(futureTrue);
		when(client.set(eq(receiver2LookupDocumentKey), eq(0), isA(String.class))).thenReturn(futureTrue);
		
		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.addMail(mailSerialized3);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client).add(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client).add(mailDocumentKey3, 0, mailSerialized3.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client).get(receiver2LookupDocumentKey);
		verify(client, times(2)).set(eq(receiver1LookupDocumentKey), eq(0), isA(String.class));
		verify(client).set(eq(receiver2LookupDocumentKey), eq(0), isA(String.class));
		verifyZeroInteractions(client);
	}
}
