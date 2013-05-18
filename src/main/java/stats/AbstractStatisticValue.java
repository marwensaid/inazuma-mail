package stats;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

public abstract class AbstractStatisticValue<VALUE>
{
	public enum Stat
	{
		SUM, COUNT, AVG, MIN, MAX
	}
	
	protected final ValueValidator valueValidator = new ValueValidator();
	protected final EnumSet<Stat> stats = EnumSet.noneOf(Stat.class);

	protected volatile VALUE lastTimeRangedValueDefault = null;
	protected volatile VALUE lastTimeRangedValueCount = null;
	protected volatile VALUE lastTimeRangedValueAvg = null;
	protected volatile VALUE lastTimeRangedValueMin = null;
	protected volatile VALUE lastTimeRangedValueMax = null;
	
	private final String group;
	private final String name;
	private final long timeRange;
	private boolean autoRegister;

	private long lastTimeRangeTick = System.nanoTime();
	
	public AbstractStatisticValue(final String group, final String name, final long duration, final TimeUnit timeUnit, final boolean autoRegister) throws IllegalArgumentException
	{
		if (name.endsWith("Cnt") || name.endsWith("Avg") || name.endsWith("Min") || name.endsWith("Max"))
		{
			throw new IllegalArgumentException("Parameter \"name\" is not allowed to end with \"Cnt\", \"Avg\", \"Min\" or \"Max\"!");
		}
		long timeRange = timeUnit.toNanos(duration);
		if (timeRange < TimeUnit.MILLISECONDS.toNanos(10))
		{
			throw new IllegalArgumentException("Parameter \"duration\" with \"timeUnit\" must be greater than 10 milliseconds!");
		}
		this.group = group;
		this.name = name;
		this.timeRange = timeRange;
		this.autoRegister = autoRegister;
	}
	
	public AbstractStatisticValue(final String name, final long duration, final TimeUnit timeUnit)
	{
		this(null, name, duration, timeUnit, true);
	}
	
	public AbstractStatisticValue(final String group, final String name, final long duration)
	{
		this(group, name, 60, TimeUnit.SECONDS, true);
	}
	
	public AbstractStatisticValue(final String name)
	{
		this(null, name, 60, TimeUnit.SECONDS, true);
	}
	
	public boolean showSum()
	{
		return stats.contains(Stat.SUM);
	}
	
	public boolean showCount()
	{
		return stats.contains(Stat.COUNT);
	}
	
	public boolean showAvg()
	{
		return stats.contains(Stat.AVG);
	}
	
	public boolean showMin()
	{
		return stats.contains(Stat.MIN);
	}

	public boolean showMax()
	{
		return stats.contains(Stat.MAX);
	}

	public boolean isMemberOf(final String group)
	{
		return (this.group == group || (this.group != null && this.group.equals(group)));
	}
	
	public String getGroup()
	{
		return group;
	}

	public String getName()
	{
		return name;
	}

	public long getTimeRange()
	{
		return timeRange;
	}

	public VALUE getLastTimeRangedValueSum()
	{
		return lastTimeRangedValueDefault;
	}
	
	public VALUE getLastTimeRangedValueCount()
	{
		return lastTimeRangedValueCount;
	}
	
	public VALUE getLastTimeRangedValueAvg()
	{
		return lastTimeRangedValueAvg;
	}
	
	public VALUE getLastTimeRangedValueMin()
	{
		return lastTimeRangedValueMin;
	}
	
	public VALUE getLastTimeRangedValueMax()
	{
		return lastTimeRangedValueMax;
	}
	
	protected void autoRegister()
	{
		// TODO get rid of getInstance() call!
		StatisticManager statisticManager = StatisticManager.getInstance();
		if (autoRegister && statisticManager != null)
		{
			statisticManager.registerStatisticValue(this);
		}
	}
	
	protected abstract String getType();
	
	protected abstract void swapValue();

	class ValueValidator implements Runnable
	{
		@Override
		public void run()
		{
			final long currentTime = System.nanoTime();
			if (currentTime - lastTimeRangeTick > timeRange)
			{
				lastTimeRangeTick = currentTime;
				swapValue();
			}
		}
	}
}
