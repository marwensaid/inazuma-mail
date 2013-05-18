package controller;

import stats.CustomStatisticValue.ValueCollector;

class QueueSizeCollector implements ValueCollector<Integer>
{
	private MailController mailController;

	public QueueSizeCollector(final MailController mailController)
	{
		this.mailController = mailController;
	}

	@Override
	public Integer collectValue()
	{
		return mailController.size();
	}

	@Override
	public String getType()
	{
		return "java.lang.Integer";
	}
}
