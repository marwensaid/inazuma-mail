package main;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory
{
	private static final UncaughtExceptionHandler UNCAUGHT_EXCEPTION_HANDLER = new LastExceptionHandler();
	private final ThreadGroup group;
	private final AtomicInteger threadNumber = new AtomicInteger(1);
	private final String namePrefix;

	public NamedThreadFactory(final String _namePrefix)
	{
		namePrefix = _namePrefix;
		SecurityManager s = System.getSecurityManager();
		group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
	}

	@Override
	public Thread newThread(Runnable r)
	{
		Thread t = new Thread(group, r, namePrefix + "-thread-" + threadNumber.getAndIncrement(), 0);
		if (t.isDaemon())
		{
			t.setDaemon(false);
		}
		if (t.getPriority() != Thread.NORM_PRIORITY)
		{
			t.setPriority(Thread.NORM_PRIORITY);
		}
		Thread.setDefaultUncaughtExceptionHandler(UNCAUGHT_EXCEPTION_HANDLER);
		return t;
	}

	private static final class LastExceptionHandler implements UncaughtExceptionHandler
	{
		@Override
		public void uncaughtException(Thread t, Throwable e)
		{
			System.err.println("Uncaught throwable in thread " + t.getName() + ": " + e.getMessage());
		}
	}
}
