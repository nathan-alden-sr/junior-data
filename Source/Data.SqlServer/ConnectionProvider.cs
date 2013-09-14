using System.Data.SqlClient;
using System.Threading.Tasks;

using Junior.Common;

namespace Junior.Data.SqlServer
{
	public class ConnectionProvider : IConnectionProvider<SqlConnection>
	{
		private readonly IConnectionStringProvider _connectionStringProvider;

		public ConnectionProvider(IConnectionStringProvider connectionStringProvider)
		{
			connectionStringProvider.ThrowIfNull("connectionStringProvider");

			_connectionStringProvider = connectionStringProvider;
		}

		public SqlConnection GetConnection(string connectionKey, bool openConnection = true)
		{
			connectionKey.ThrowIfNull("connectionKey");

			var connection = new SqlConnection(_connectionStringProvider.ByKey(connectionKey));

			if (openConnection)
			{
				connection.Open();
			}

			return connection;
		}

		public async Task<SqlConnection> GetConnectionAsync(string connectionKey, bool openConnection = true)
		{
			connectionKey.ThrowIfNull("connectionKey");

			var connection = new SqlConnection(_connectionStringProvider.ByKey(connectionKey));

			if (openConnection)
			{
				await connection.OpenAsync();
			}

			return connection;
		}
	}
}