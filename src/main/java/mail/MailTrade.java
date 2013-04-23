package mail;

import java.util.List;

public class MailTrade extends Mail
{
	private final List<Long> items;
	
	public MailTrade(final int senderID, final int receiverID, final List<Long> items)
	{
		super(MailType.TRADE, senderID, receiverID);
		this.items = items;
	}

	public List<Long> getItems()
	{
		return items;
	}
}
