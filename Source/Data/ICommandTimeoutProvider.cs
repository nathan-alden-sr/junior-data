using System;

namespace Junior.Data
{
	public interface ICommandTimeoutProvider
	{
		TimeSpan GetTimeout(string connectionKey);
	}
}