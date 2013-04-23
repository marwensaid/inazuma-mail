package mail;

import java.util.UUID;

public abstract class Mail
{
	protected final transient UUID key;
	protected final MailType mailType;
	protected final int senderID;
	protected final int receiverID;
	protected final long created;
	
	protected Mail(final MailType mailType, final int senderID, final int receiverID)
	{
		this.key = UUID.randomUUID();
		this.mailType = mailType;
		this.senderID = senderID;
		this.receiverID = receiverID;
		this.created = System.currentTimeMillis() / 1000;
	}
	
	public String getKey()
	{
		return key.toString();
	}

	public MailType getMailType()
	{
		return mailType;
	}

	public int getSenderID()
	{
		return senderID;
	}

	public int getReceiverID()
	{
		return receiverID;
	}

	public long getCreated()
	{
		return created;
	}
}
