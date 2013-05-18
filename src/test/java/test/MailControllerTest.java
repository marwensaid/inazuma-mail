package test;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.powermock.reflect.Whitebox;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.couchbase.client.CouchbaseClient;

import controller.MailController;

public class MailControllerTest
{
	CouchbaseClient client;
	MailController mailController;

	@BeforeMethod
	public void setUp()
	{
		client = mock(CouchbaseClient.class);
		mailController = new MailController(client, 1, 1);
	}

	@Test
	public void shutdownAwaitLoopException() throws InterruptedException
	{
		final CountDownLatch latch = mock(CountDownLatch.class);
		when(latch.await(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException());

		Whitebox.setInternalState(mailController, CountDownLatch.class, latch);

		mailController.awaitShutdown();

		verify(latch).await(anyLong(), any(TimeUnit.class));
		verifyZeroInteractions(latch);
	}
}
