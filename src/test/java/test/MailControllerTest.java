package test;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


import org.powermock.reflect.Whitebox;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import com.couchbase.client.CouchbaseClient;

import controller.MailController;
import controller.MailControllerQueueSizeThread;

public class MailControllerTest
{
	CouchbaseClient client;

	MailController mailStorageQueue;

	@BeforeMethod
	public void setUp()
	{
		client = mock(CouchbaseClient.class);

		mailStorageQueue = new MailController(client, 1, 1);
	}

	@Test
	public void queueSize() throws InterruptedException
	{
		final ScheduledExecutorService threadPool = mock(ScheduledExecutorService.class);
		final MailController queue = spy(mailStorageQueue);
		when(queue.size()).thenReturn(5).thenReturn(10).thenReturn(0);

		MailControllerQueueSizeThread queueSize = new MailControllerQueueSizeThread(threadPool, queue, 5L, TimeUnit.SECONDS);
		queueSize.run();
		queueSize.run();
		queueSize.run();

		verify(threadPool, times(3)).schedule(any(MailControllerQueueSizeThread.class), eq(5L), eq(TimeUnit.SECONDS));
		verifyZeroInteractions(threadPool);
		verify(queue, times(3)).size();
		verifyZeroInteractions(queue);
	}

	@Test
	public void shutdownAwaitLoopException() throws InterruptedException
	{
		final CountDownLatch latch = mock(CountDownLatch.class);
		when(latch.await(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException());

		Whitebox.setInternalState(mailStorageQueue, CountDownLatch.class, latch);

		mailStorageQueue.awaitShutdown();

		verify(latch).await(anyLong(), any(TimeUnit.class));
		verifyZeroInteractions(latch);
	}
}
