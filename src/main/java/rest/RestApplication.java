package rest;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

@WebListener
@ApplicationPath("api")
public class RestApplication extends Application implements ServletContextListener
{
	public RestApplication()
	{
	}

	@Override
	public void contextDestroyed(ServletContextEvent arg0)
	{
	}

	@Override
	public void contextInitialized(ServletContextEvent arg0)
	{
	}
}
