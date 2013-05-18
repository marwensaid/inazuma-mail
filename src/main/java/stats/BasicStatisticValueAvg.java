package stats;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.ejb.EJB;

public class BasicStatisticValueAvg extends AbstractStatisticValue<Long>
{
	private final AtomicLong timeRangeValue = new AtomicLong(0);
	private final AtomicLong invocations = new AtomicLong(0);

	@EJB
	private StatisticManager statisticManager;
	
	public BasicStatisticValueAvg(final String group, final String name, final long duration, final TimeUnit timeUnit, final boolean autoRegister)
	{
		super(group, name, duration, timeUnit, autoRegister);
		lastTimeRangedValueAvg = 0L;
		stats.add(Stat.AVG);
		if (autoRegister && statisticManager != null)
		{
			statisticManager.registerStatisticValue(this);
		}
	}

	public BasicStatisticValueAvg(final String group, final String name, final long duration, final TimeUnit timeUnit)
	{
		this(group, name, duration, timeUnit, true);
	}

	public BasicStatisticValueAvg(final String name, final long duration, final TimeUnit timeUnit)
	{
		this(null, name, duration, timeUnit, true);
	}
	
	public BasicStatisticValueAvg(final String group, final String name)
	{
		this(group, name, 60, TimeUnit.SECONDS, true);
	}

	public BasicStatisticValueAvg(final String name)
	{
		this(null, name);
	}

	public void increment(final long value)
	{
		timeRangeValue.addAndGet(value);
		invocations.incrementAndGet();
	}
	
	@Override
	protected String getType()
	{
		return "java.lang.Long";
	}

	@Override
	protected void swapValue()
	{
		final double value = timeRangeValue.getAndSet(0);
		final long inv = invocations.getAndSet(0);
		lastTimeRangedValueAvg = (inv == 0) ? 0L : Math.round(value / inv); 
	}
}
