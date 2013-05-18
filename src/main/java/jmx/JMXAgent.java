package jmx;

import java.lang.management.ManagementFactory;

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.management.MBeanServer;

import stats.StatisticManager;

@Startup
@Singleton
public class JMXAgent
{
	@EJB
	private StatisticManager statisticManager;
	
	@PostConstruct
	protected void init()
	{
		try
		{
			final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

			// Provide StatisticManager with data for JMX agent 
			statisticManager.registerMBean(mbs, "com.thunderphreak:type=StatisticManager");
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
}
