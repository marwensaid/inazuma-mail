package database;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;
import javax.ejb.Singleton;

import com.couchbase.client.CouchbaseClient;

@Singleton
public class ConnectionManager
{
	private final CouchbaseClient client;
	
	public ConnectionManager()
	{
		// Set the URIs and get a client
		List<URI> uris = new LinkedList<URI>();

		// Connect to localhost or to the appropriate URI(s)
		uris.add(URI.create("http://127.0.0.1:8091/pools"));

		CouchbaseClient tmpClient = null;
		try
		{
			// Use the "mail" bucket with no password
			tmpClient = new CouchbaseClient(uris, "mail", "");
		}
		catch (IOException e)
		{
			System.err.println("IOException connecting to Couchbase: " + e.getMessage());
			System.exit(1);
		}
		finally
		{
			client = tmpClient;
		}
	}
	
	public final CouchbaseClient getConnection()
	{
		return client;
	}
	
	@PreDestroy
	public final void shutdown()
	{
		client.shutdown(60, TimeUnit.SECONDS);
	}
}
