using System.Threading.Tasks;

using Junior.Common;

using Npgsql;

namespace Junior.Data.PostgreSql
{
	public class ConnectionProvider : IConnectionProvider<NpgsqlConnection>
	{
		private readonly IConnectionStringProvider _connectionStringProvider;

		public ConnectionProvider(IConnectionStringProvider connectionStringProvider)
		{
			connectionStringProvider.ThrowIfNull("connectionStringProvider");

			_connectionStringProvider = connectionStringProvider;
		}

		public NpgsqlConnection GetConnection(string connectionKey, bool openConnection = true)
		{
			connectionKey.ThrowIfNull("connectionKey");

			var connection = new NpgsqlConnection(_connectionStringProvider.ByKey(connectionKey));

			if (openConnection)
			{
				connection.Open();
			}

			return connection;
		}

		public async Task<NpgsqlConnection> GetConnectionAsync(string connectionKey, bool openConnection = true)
		{
			connectionKey.ThrowIfNull("connectionKey");

			var connection = new NpgsqlConnection(_connectionStringProvider.ByKey(connectionKey));

			if (openConnection)
			{
				await connection.OpenAsync();
			}

			return connection;
		}
	}
}