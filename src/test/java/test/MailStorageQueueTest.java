package test;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import mail.MailUser;
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.internal.OperationFuture;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import queue.MailStorageQueue;
import queue.ReceiverLookupDocument;
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

	ReceiverLookupDocument receiverLookupDocument1;
	String receiverLookupDocument1JSON;

	ReceiverLookupDocument receiverLookupDocument1And2;
	String receiverLookupDocument1And2JSON;

	ReceiverLookupDocument receiverLookupDocument3;
	String receiverLookupDocument3JSON;

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

		receiverLookupDocument1 = new ReceiverLookupDocument();
		receiverLookupDocument1.add(mailSerialized1.getCreated(), mailSerialized1.getKey());
		receiverLookupDocument1JSON = receiverLookupDocument1.toJSON();

		receiverLookupDocument1And2 = new ReceiverLookupDocument();
		receiverLookupDocument1And2.add(mailSerialized1.getCreated(), mailSerialized1.getKey());
		receiverLookupDocument1And2.add(mailSerialized2.getCreated(), mailSerialized2.getKey());
		receiverLookupDocument1And2JSON = receiverLookupDocument1And2.toJSON();

		receiverLookupDocument3 = new ReceiverLookupDocument();
		receiverLookupDocument3.add(mailSerialized3.getCreated(), mailSerialized3.getKey());
		receiverLookupDocument3JSON = receiverLookupDocument3.toJSON();

		futureTrue = mock(OperationFuture.class);
		when(futureTrue.get()).thenReturn(true);
		futureFalse = mock(OperationFuture.class);
		when(futureFalse.get()).thenReturn(false);
	}

	@Test(timeOut = 1000)
	public void addFirstMail()
	{
		when(client.set(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON))).thenReturn(futureTrue);

		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client).set(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client).set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON));
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void addSecondMail()
	{
		when(client.set(mailDocumentKey2, 0, mailSerialized2.getDocument())).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(receiverLookupDocument1JSON);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1And2JSON))).thenReturn(futureTrue);

		mailStorageQueue.addMail(mailSerialized2);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client).set(mailDocumentKey2, 0, mailSerialized2.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client).set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1And2JSON));
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void persistMailExceptionOnce()
	{
		when(client.set(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenThrow(new IllegalStateException()).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON))).thenReturn(futureTrue);

		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client, times(2)).set(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client).set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON));
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void persistMailFailsOnce()
	{
		when(client.set(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenReturn(futureFalse).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON))).thenReturn(futureTrue);

		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client, times(2)).set(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client).set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON));
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void persistLookupExcptionOnce()
	{
		when(client.set(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON))).thenThrow(new IllegalStateException()).thenReturn(futureTrue);

		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client).set(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client, times(2)).set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON));
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void persistLookupFailsOnce()
	{
		when(client.set(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON))).thenReturn(futureFalse).thenReturn(futureTrue);

		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client).set(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client, times(2)).set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON));
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void persistLookupFailsTwice()
	{
		when(client.set(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON))).thenReturn(futureFalse).thenReturn(futureFalse).thenReturn(futureTrue);

		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client).set(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client, times(3)).set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON));
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void persistLookupFailsOnceWithSecondMailOnQueueWithSameReceiver()
	{
		when(client.set(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenReturn(futureTrue);
		when(client.set(mailDocumentKey2, 0, mailSerialized2.getDocument())).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON))).thenReturn(futureFalse);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1And2JSON))).thenReturn(futureTrue);

		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.addMail(mailSerialized2);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client).set(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client).set(mailDocumentKey2, 0, mailSerialized2.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client).set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON));
		verify(client).set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1And2JSON));
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void persistLookupFailsOnceWithSecondMailOnQueueWithDifferentReceiver()
	{
		when(client.set(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenReturn(futureTrue);
		when(client.set(mailDocumentKey3, 0, mailSerialized3.getDocument())).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(null);
		when(client.get(receiver2LookupDocumentKey)).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON))).thenReturn(futureFalse).thenReturn(futureTrue);
		when(client.set(eq(receiver2LookupDocumentKey), eq(0), eq(receiverLookupDocument3JSON))).thenReturn(futureTrue);

		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.addMail(mailSerialized3);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client).set(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client).set(mailDocumentKey3, 0, mailSerialized3.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client).get(receiver2LookupDocumentKey);
		verify(client, times(2)).set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON));
		verify(client).set(eq(receiver2LookupDocumentKey), eq(0), eq(receiverLookupDocument3JSON));
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void getLookupDocumentExceptionOnce()
	{
		when(client.set(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenThrow(new OperationTimeoutException("Operation timeout")).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON))).thenReturn(futureTrue);

		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client).set(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client, times(2)).get(receiver1LookupDocumentKey);
		verify(client).set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON));
		verifyZeroInteractions(client);
	}

	@Test
	public void shutdownAwaitLoop() throws InterruptedException, IllegalAccessException, NoSuchFieldException
	{
		final CountDownLatch latch = mock(CountDownLatch.class);
		when(latch.getCount()).thenReturn(2L).thenReturn(1L).thenReturn(0L);
		changePrivateFinalField(mailStorageQueue, "latch", latch);

		MailStorageQueue queue = spy(mailStorageQueue);
		when(queue.size()).thenReturn(5).thenReturn(10).thenReturn(0);

		queue.shutdown();
		queue.awaitShutdown();

		verify(latch).countDown();
		verify(latch, times(3)).await(anyLong(), any(TimeUnit.class));
		verify(latch, times(3)).getCount();
		verifyZeroInteractions(latch);
	}
	
	@Test
	public void shutdownAwaitLoopException() throws InterruptedException, IllegalAccessException, NoSuchFieldException
	{
		final CountDownLatch latch = mock(CountDownLatch.class);
		when(latch.await(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException());
		changePrivateFinalField(mailStorageQueue, "latch", latch);

		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(latch).await(anyLong(), any(TimeUnit.class));
		verifyZeroInteractions(latch);
	}

	private void changePrivateFinalField(Object instance, String fieldName, Object newValue) throws IllegalAccessException, NoSuchFieldException
	{
		Field field = instance.getClass().getDeclaredField(fieldName);
		Field modifiersField = Field.class.getDeclaredField("modifiers");

		modifiersField.setAccessible(true);
		modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

		field.setAccessible(true);
		field.set(instance, newValue);
	}
}
