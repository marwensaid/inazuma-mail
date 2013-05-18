package stats;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class AdvancedStatisticValue extends AbstractStatisticValue<Long>
{
	private final AtomicLong timeRangeValue = new AtomicLong(0);
	private final AtomicLong timeRangeValueMin = new AtomicLong(Long.MAX_VALUE);
	private final AtomicLong timeRangeValueMax = new AtomicLong(Long.MIN_VALUE);
	private final AtomicLong invocations = new AtomicLong(0);

	public AdvancedStatisticValue(final String group, final String name, final long duration, final TimeUnit timeUnit, final EnumSet<Stat> options, final boolean autoRegister)
	{
		super(group, name, duration, timeUnit, autoRegister);
		stats.addAll(options);
		if (showSum()) lastTimeRangedValueDefault = 0L;
		if (showCount()) lastTimeRangedValueCount = 0L;
		if (showAvg()) lastTimeRangedValueAvg = 0L;
		if (showMin()) lastTimeRangedValueMin = 0L;
		if (showMax()) lastTimeRangedValueMax = 0L;
		autoRegister();
	}
	
	public AdvancedStatisticValue(final String group, final String name, final long duration, final TimeUnit timeUnit, final EnumSet<Stat> options)
	{
		this(group, name, duration, timeUnit, options, true);
	}

	public AdvancedStatisticValue(final String name, final long duration, final TimeUnit timeUnit, final EnumSet<Stat> options)
	{
		this(null, name, duration, timeUnit, options, true);
	}
	
	public AdvancedStatisticValue(final String group, final String name, final EnumSet<Stat> options)
	{
		this(group, name, 60, TimeUnit.SECONDS, options);
	}

	public AdvancedStatisticValue(final String name, final EnumSet<Stat> options)
	{
		this(null, name, options);
	}

	public void increment(final long value)
	{
		timeRangeValue.addAndGet(value);
		invocations.incrementAndGet();
		final long oldMin = timeRangeValueMin.get();
		if (value < oldMin)
		{
			timeRangeValueMin.compareAndSet(oldMin, value);
		}
		final long oldMax = timeRangeValueMax.get();
		if (value > oldMax)
		{
			timeRangeValueMax.compareAndSet(oldMax, value);
		}
	}
	
	@Override
	protected String getType()
	{
		return "java.lang.Long";
	}

	@Override
	protected void swapValue()
	{
		final long value = timeRangeValue.getAndSet(0);
		final long inv = invocations.getAndSet(0);
		if (showSum())
		{
			lastTimeRangedValueDefault = value;
		}
		if (showCount())
		{
			lastTimeRangedValueCount = inv;
		}
		if (showAvg())
		{
			lastTimeRangedValueAvg = (inv == 0) ? 0L : Math.round((double)value / inv); 
		}
		if (showMin())
		{
			final long showMin = timeRangeValueMin.getAndSet(Long.MAX_VALUE);
			lastTimeRangedValueMin = (showMin == Long.MAX_VALUE) ? 0L : showMin; 
		}
		if (showMax())
		{
			final long showMax = timeRangeValueMax.getAndSet(Long.MIN_VALUE);
			lastTimeRangedValueMax = (showMax == Long.MIN_VALUE) ? 0L : showMax; 
		}
	}
}
