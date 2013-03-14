using System.Configuration;

using Junior.Common;

namespace Junior.Data.PostgreSql
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