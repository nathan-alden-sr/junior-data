using System.Data;
using System.Threading.Tasks;

using Junior.Common;

using Npgsql;

namespace Junior.Data.PostgreSql
{
	public class ConnectionContextFactory : IConnectionContextFactory<ConnectionContext>
	{
		private readonly IConnectionProvider<NpgsqlConnection> _connectionProvider;

		public ConnectionContextFactory(IConnectionProvider<NpgsqlConnection> connectionProvider)
		{
			connectionProvider.ThrowIfNull("connectionProvider");

			_connectionProvider = connectionProvider;
		}

		public async Task<ConnectionContext> Create(string connectionKey)
		{
			connectionKey.ThrowIfNull("connectionKey");

			NpgsqlConnection connection = await _connectionProvider.GetConnection(connectionKey, false);

			return new ConnectionContext(connectionKey, connection, null, TransactionDisposeBehavior.None);
		}

		public async Task<ConnectionContext> CreateWithTransaction(string connectionKey, TransactionDisposeBehavior transactionDisposeBehavior = TransactionDisposeBehavior.RollbackIfNonFinalized)
		{
			connectionKey.ThrowIfNull("connectionKey");

			NpgsqlConnection connection = await _connectionProvider.GetConnection(connectionKey, true);
			NpgsqlTransaction transaction = connection.BeginTransaction();

			return new ConnectionContext(connectionKey, connection, transaction, transactionDisposeBehavior);
		}

		public async Task<ConnectionContext> CreateWithTransaction(
			string connectionKey,
			IsolationLevel isolationLevel,
			TransactionDisposeBehavior transactionDisposeBehavior = TransactionDisposeBehavior.RollbackIfNonFinalized)
		{
			connectionKey.ThrowIfNull("connectionKey");

			NpgsqlConnection connection = await _connectionProvider.GetConnection(connectionKey, true);
			NpgsqlTransaction transaction = connection.BeginTransaction(isolationLevel);

			return new ConnectionContext(connectionKey, connection, transaction, transactionDisposeBehavior);
		}
	}
}