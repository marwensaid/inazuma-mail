package mail;

public class MailUser extends Mail
{
	private final String subject;
	private final String body;
	
	public MailUser(final int senderID, final int receiverID, final String subject, final String body)
	{
		super(MailType.USER, senderID, receiverID);
		this.subject = subject;
		this.body = body;
	}

	public String getSubject()
	{
		return subject;
	}

	public String getBody()
	{
		return body;
	}
}