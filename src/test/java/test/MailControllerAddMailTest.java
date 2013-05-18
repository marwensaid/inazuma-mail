package test;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import model.ReceiverLookupDocument;
import model.SerializedMail;
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.internal.OperationFuture;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.couchbase.client.CouchbaseClient;

import controller.MailController;

public class MailControllerAddMailTest
{
	private final static int ANY_RECEIVER_1 = 20;
	private final static int ANY_RECEIVER_2 = 30;

	Random generator;
	
	CouchbaseClient client;
	MailController mailStorageQueue;

	SerializedMail mailSerialized1;
	String mailDocumentKey1;

	SerializedMail mailSerialized2;
	String mailDocumentKey2;

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

	@Mock
	OperationFuture<Boolean> futureTrue;
	@Mock
	OperationFuture<Boolean> futureFalse;

	@BeforeMethod
	public void setUp() throws InterruptedException, ExecutionException
	{
		generator = new Random();
		
		client = mock(CouchbaseClient.class);
		mailStorageQueue = new MailController(client, 1, 1);

		mailSerialized1 = createSerializedMail(ANY_RECEIVER_1, "{content:\"mail1\"}");
		mailDocumentKey1 = "mail_" + mailSerialized1.getKey();
		
		mailSerialized2 = createSerializedMail(ANY_RECEIVER_1, "{content:\"mail2\"}");
		mailDocumentKey2 = "mail_" + mailSerialized2.getKey();

		mailSerialized3 = createSerializedMail(ANY_RECEIVER_2, "{content:\"mail3\"}");
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

		MockitoAnnotations.initMocks(this);
		when(futureTrue.get()).thenReturn(true);
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
	public void addSameMailTwice()
	{
		when(client.set(mailDocumentKey1, 0, mailSerialized1.getDocument())).thenReturn(futureTrue);
		when(client.get(receiver1LookupDocumentKey)).thenReturn(null);
		when(client.set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON))).thenReturn(futureTrue);

		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.addMail(mailSerialized1);
		mailStorageQueue.shutdown();
		mailStorageQueue.awaitShutdown();

		verify(client, times(2)).set(mailDocumentKey1, 0, mailSerialized1.getDocument());
		verify(client).get(receiver1LookupDocumentKey);
		verify(client).set(eq(receiver1LookupDocumentKey), eq(0), eq(receiverLookupDocument1JSON));
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

	private SerializedMail createSerializedMail(final int receiverID, final String document)
	{
		final long created = (System.currentTimeMillis() / 1000) - generator.nextInt(86400);
		return new SerializedMail(receiverID, created, UUID.randomUUID().toString(), document);
	}
}
