package main;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.CouchbaseClient;

public class ClusterStatusJob implements Runnable
{
	private final static List<String> statNames = new ArrayList<String>();
	
	static
	{
		statNames.add("vb_active_num_ref_ejects");
		statNames.add("vb_active_num_non_resident");
		statNames.add("vb_active_eject");
		statNames.add("ep_num_non_resident");
		statNames.add("ep_uncommitted_items");
	}
	
	private final ScheduledExecutorService threadPool;
	private final CouchbaseClient client;
	
	public ClusterStatusJob(final ScheduledExecutorService threadPool, final CouchbaseClient client)
	{
		this.threadPool = threadPool;
		this.client = client;
	}

	@Override
	public void run()
	{
		if (threadPool.isShutdown() || threadPool.isTerminated())
		{
			return;
		}
		
		StringBuilder sb = new StringBuilder("Cluster stats:");
		for (SocketAddress socket : client.getStats().keySet())
		{
			Map<String, String> stats = client.getStats().get(socket); 
			for (String statName : statNames)
			{
				sb.append(" ").append(statName).append(":").append(stats.get(statName));
			}
		}
		System.out.println(sb.toString());
		
		if (!threadPool.isShutdown() && !threadPool.isTerminated())
		{
			threadPool.schedule(this, 5, TimeUnit.SECONDS);
		}
	}
}
