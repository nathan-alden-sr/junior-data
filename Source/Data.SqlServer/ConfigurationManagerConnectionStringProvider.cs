using System.Configuration;

using Junior.Common;

namespace Junior.Data.SqlServer
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