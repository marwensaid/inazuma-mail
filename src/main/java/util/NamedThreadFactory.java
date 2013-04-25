package util;

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
		Thread.setDefaultUncaughtExceptionHandler(UNCAUGHT_EXCEPTION_HANDLER);
		return new Thread(group, r, namePrefix + "-thread-" + threadNumber.getAndIncrement(), 0);
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
