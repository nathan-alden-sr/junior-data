using System.Threading.Tasks;

using Junior.Common;

using MySql.Data.MySqlClient;

namespace Junior.Data.MySql
{
	public class ConnectionProvider : IConnectionProvider<MySqlConnection>
	{
		private readonly IConnectionStringProvider _connectionStringProvider;

		public ConnectionProvider(IConnectionStringProvider connectionStringProvider)
		{
			connectionStringProvider.ThrowIfNull("connectionStringProvider");

			_connectionStringProvider = connectionStringProvider;
		}

		public MySqlConnection GetConnection(string connectionKey, bool openConnection = true)
		{
			connectionKey.ThrowIfNull("connectionKey");

			var connection = new MySqlConnection(_connectionStringProvider.ByKey(connectionKey));

			if (openConnection)
			{
				connection.Open();
			}

			return connection;
		}

		public async Task<MySqlConnection> GetConnectionAsync(string connectionKey, bool openConnection = true)
		{
			connectionKey.ThrowIfNull("connectionKey");

			var connection = new MySqlConnection(_connectionStringProvider.ByKey(connectionKey));

			if (openConnection)
			{
				await connection.OpenAsync();
			}

			return connection;
		}
	}
}