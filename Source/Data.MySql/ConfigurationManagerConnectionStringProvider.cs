using System.Configuration;

using Junior.Common;

namespace Junior.Data.MySql
{
	public class ConfigurationManagerConnectionStringProvider : IConnectionStringProvider
	{
		public string ByKey(string key)
		{
			key.ThrowIfNull("key");

			return ConfigurationManager.ConnectionStrings[key].ConnectionString;
		}
	}
}