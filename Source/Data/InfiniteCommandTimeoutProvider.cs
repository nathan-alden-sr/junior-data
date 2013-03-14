using System;

namespace Junior.Data
{
	public class InfiniteCommandTimeoutProvider : ICommandTimeoutProvider
	{
		public TimeSpan GetTimeout(string connectionKey)
		{
			return TimeSpan.Zero;
		}
	}
}