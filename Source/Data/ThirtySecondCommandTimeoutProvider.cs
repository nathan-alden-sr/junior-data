using System;

namespace Junior.Data
{
	public class ThirtySecondCommandTimeoutProvider : ICommandTimeoutProvider
	{
		public TimeSpan GetTimeout(string connectionKey)
		{
			return TimeSpan.FromSeconds(30);
		}
	}
}