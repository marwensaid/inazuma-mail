package test;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import queue.MailStorageQueue;
import queue.MailStorageQueueSizeThread;

import com.couchbase.client.CouchbaseClient;

public class MailStorageQueueTest
{
	CouchbaseClient client;

	MailStorageQueue mailStorageQueue;
	
	@BeforeMethod
	public void setUp() throws InterruptedException, ExecutionException
	{
		client = mock(CouchbaseClient.class);
		mailStorageQueue = new MailStorageQueue(client, 1, 1);
	}
	
	@Test
	public void queueSize() throws InterruptedException
	{
		final ScheduledExecutorService threadPool = mock(ScheduledExecutorService.class);
		final MailStorageQueue queue = spy(mailStorageQueue);
		when(queue.size()).thenReturn(5).thenReturn(10).thenReturn(0);
		
		MailStorageQueueSizeThread queueSize = new MailStorageQueueSizeThread(threadPool, queue, 5L, TimeUnit.SECONDS);
		queueSize.run();
		queueSize.run();
		queueSize.run();
		
		verify(threadPool, times(3)).schedule(any(MailStorageQueueSizeThread.class), eq(5L),eq(TimeUnit.SECONDS));
		verifyZeroInteractions(threadPool);
		verify(queue, times(3)).size();
		verifyZeroInteractions(queue);
	}
	
	@Test
	public void shutdownAwaitLoopException() throws InterruptedException, IllegalAccessException, NoSuchFieldException
	{
		final CountDownLatch latch = mock(CountDownLatch.class);
		when(latch.await(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException());
		changePrivateFinalField(mailStorageQueue, "latch", latch);

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
