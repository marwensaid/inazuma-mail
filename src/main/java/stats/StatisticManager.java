package stats;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;

@Startup
@Singleton
public class StatisticManager
{
	private static StatisticManager INSTANCE = null;
	
	private final Map<String, AbstractStatisticValue<?>> statisticValues = new HashMap<String, AbstractStatisticValue<?>>();
	private final Set<String> statisticGroups = new HashSet<String>();
	private final Lock lock = new ReentrantLock();

	private final AtomicBoolean running = new AtomicBoolean(true);
	private final Thread collectionThread = new Thread(new CollectionTask(), "StatisticCollector");
	
	private MBeanServer mbs = null;
	private String baseName = null;

	public StatisticManager()
	{
		collectionThread.start();
		INSTANCE = this;
	}
	
	public static StatisticManager getInstance()
	{
		return INSTANCE;
	}

	public void shutdown()
	{
		running.set(false);
		collectionThread.interrupt();
	}
	
	public Attribute getStatisticValue(final String group, final String attribute)
	{
		try
		{
			final boolean isCount = attribute.endsWith("Cnt");
			final boolean isAvg = attribute.endsWith("Avg");
			final boolean isMin = attribute.endsWith("Min");
			final boolean isMax = attribute.endsWith("Max");
			final boolean isSum = (!isCount && !isAvg && !isMin && !isMax);
			final String attributeSearch = isSum ? attribute : attribute.substring(0, attribute.length() - 3);
			
			lock.lock();
			final AbstractStatisticValue<?> statisticValue = statisticValues.get(attributeSearch);
			if (statisticValue != null && statisticValue.isMemberOf(group))
			{
				if (isSum && statisticValue.showSum())
				{
					return new Attribute(attribute, statisticValue.getLastTimeRangedValueSum());
				}
				if (isCount && statisticValue.showCount())
				{
					return new Attribute(attribute, statisticValue.getLastTimeRangedValueCount());
				}
				if (isAvg && statisticValue.showAvg())
				{
					return new Attribute(attribute, statisticValue.getLastTimeRangedValueAvg());
				}
				if (isMin && statisticValue.showMin())
				{
					return new Attribute(attribute, statisticValue.getLastTimeRangedValueMin());
				}
				if (isMax && statisticValue.showMax())
				{
					return new Attribute(attribute, statisticValue.getLastTimeRangedValueMax());
				}
			}
			return null;
		}
		finally
		{
			lock.unlock();
		}
	}
	
	public void registerStatisticValue(final AbstractStatisticValue<?> statisticValue)
	{
		try
		{
			lock.lock();
			AbstractStatisticValue<?> oldValue = statisticValues.put(statisticValue.getName(), statisticValue);
			if (oldValue != null)
			{
				throw new IllegalArgumentException("Cannot register statistic value with name " + statisticValue.getName() + " twice! Old group: " + oldValue.getGroup() + " New group: " + statisticValue.getGroup());
			}
			registerMBean(statisticValue.getGroup());
		}
		finally
		{
			lock.unlock();
		}
	}

	public void registerMBean(final MBeanServer mbs, final String baseName)
	{
		try
		{
			lock.lock();
			this.mbs = mbs;
			this.baseName = baseName;
			try
			{
				final ObjectName objectName = new ObjectName(baseName);
				if (!mbs.isRegistered(objectName))
				{
					mbs.registerMBean(new StatisticManagerMBean(), objectName);
				}
			}
			catch (Exception e)
			{
				// Intentionally left blank
			}
		}
		finally
		{
			lock.unlock();
		}
	}
	
	private void registerMBean(final String group)
	{
		if (mbs == null || baseName == null || group == null)
		{
			return;
		}
		if (statisticGroups.add(group))
		{
			try
			{
				final ObjectName objectName = new ObjectName(baseName + ",name=" + group);
				if (!mbs.isRegistered(objectName))
				{
					mbs.registerMBean(new StatisticManagerMBean(group), objectName);
				}
			}
			catch (Exception e)
			{
				// Intentionally left blank
			}
		}
	}
	
	private class StatisticManagerMBean implements DynamicMBean
	{
		private final String group;
		
		public StatisticManagerMBean()
		{
			this.group = null;
		}
		
		public StatisticManagerMBean(final String group)
		{
			this.group = group;
		}
		
		@Override
		public Object getAttribute(final String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException
		{
			if (attribute == null || attribute.length() == 0)
			{
				throw new AttributeNotFoundException();
			}
			return getStatisticValue(group, attribute);
		}

		@Override
		public void setAttribute(final Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException
		{
		}

		@Override
		public AttributeList getAttributes(final String[] attributes)
		{
			if (attributes == null || attributes.length == 0)
			{
				return null;
			}
			try
			{
				final AttributeList list = new AttributeList();
				lock.lock();
				for (int i = 0; i < attributes.length; i++)
				{
					final Attribute attribute = getStatisticValue(group, attributes[i]);
					if (attribute != null)
					{
						list.add(attribute);
					}
				}
				return list;
			}
			catch (Exception e)
			{
				return null;
			}
			finally
			{
				lock.unlock();
			}
		}

		@Override
		public AttributeList setAttributes(final AttributeList attributes)
		{
			return null;
		}

		@Override
		public Object invoke(final String actionName, final Object[] params, final String[] signature) throws MBeanException, ReflectionException
		{
			return null;
		}
		
		@Override
		public MBeanInfo getMBeanInfo()
		{
			try
			{
				lock.lock();
				final List<MBeanAttributeInfo> attrs = new LinkedList<MBeanAttributeInfo>();
				final Iterator<String> iterator = statisticValues.keySet().iterator();
				while (iterator.hasNext())
				{
					final String attribute = iterator.next();
					final AbstractStatisticValue<?> statisticValue = statisticValues.get(attribute);
					if (statisticValue != null && statisticValue.isMemberOf(group))
					{
						if (statisticValue.showSum())
						{
							attrs.add(createAttributeInfo(attribute, statisticValue.getType(), "", " (sum)"));
						}
						if (statisticValue.showCount())
						{
							attrs.add(createAttributeInfo(attribute, statisticValue.getType(), "Cnt", " (count)"));
						}
						if (statisticValue.showAvg())
						{
							attrs.add(createAttributeInfo(attribute, statisticValue.getType(), "Avg", " (average)"));
						}
						if (statisticValue.showMin())
						{
							attrs.add(createAttributeInfo(attribute, statisticValue.getType(), "Min", " (minimum)"));
						}
						if (statisticValue.showMax())
						{
							attrs.add(createAttributeInfo(attribute, statisticValue.getType(), "Max", " (maximum)"));
						}
					}
				}
				return new MBeanInfo(
					this.getClass().getName(),
					"StatisticManager Wrapper MBean" + (group != null ? " (" + group + ")" : ""),
					attrs.toArray(new MBeanAttributeInfo[attrs.size()]),
					null,
					null,
					null
				);
			}
			finally
			{
				lock.unlock();
			}
		}

		private MBeanAttributeInfo createAttributeInfo(final String attribute, final String type, final String postfix, final String propertyPostfix)
		{
			return new MBeanAttributeInfo(
				attribute + postfix,
				type,
				"Property " + attribute + propertyPostfix,
				true, // isReadable
				false, // isWritable
				false  // isIs
			);
		}
	}

	private class CollectionTask implements Runnable
	{
		@Override
		public void run()
		{
			while (running.get())
			{
				try
				{
					lock.lock();
					final Iterator<String> iterator = statisticValues.keySet().iterator();
					while (iterator.hasNext())
					{
						final String attribute = iterator.next();
						statisticValues.get(attribute).valueValidator.run();
					}
				}
				finally
				{
					lock.unlock();
				}
				try
				{
					Thread.sleep(10);
				}
				catch (InterruptedException e)
				{
					// Intentionally left blank
				}
			}
		}
	}
}
