using System.Data;
using System.Threading.Tasks;

using Junior.Common;

using MySql.Data.MySqlClient;

namespace Junior.Data.MySql
{
	public class ConnectionContextFactory : IConnectionContextFactory<ConnectionContext>
	{
		private readonly IConnectionProvider<MySqlConnection> _connectionProvider;

		public ConnectionContextFactory(IConnectionProvider<MySqlConnection> connectionProvider)
		{
			connectionProvider.ThrowIfNull("connectionProvider");

			_connectionProvider = connectionProvider;
		}

		public async Task<ConnectionContext> CreateAsync(string connectionKey)
		{
			connectionKey.ThrowIfNull("connectionKey");

			MySqlConnection connection = await _connectionProvider.GetConnectionAsync(connectionKey, false);

			return new ConnectionContext(connectionKey, connection, null, TransactionDisposeBehavior.None);
		}

		public async Task<ConnectionContext> CreateWithTransactionAsync(string connectionKey, TransactionDisposeBehavior transactionDisposeBehavior = TransactionDisposeBehavior.RollbackIfNonFinalized)
		{
			connectionKey.ThrowIfNull("connectionKey");

			MySqlConnection connection = await _connectionProvider.GetConnectionAsync(connectionKey, true);
			MySqlTransaction transaction = connection.BeginTransaction();

			return new ConnectionContext(connectionKey, connection, transaction, transactionDisposeBehavior);
		}

		public async Task<ConnectionContext> CreateWithTransactionAsync(
			string connectionKey,
			IsolationLevel isolationLevel,
			TransactionDisposeBehavior transactionDisposeBehavior = TransactionDisposeBehavior.RollbackIfNonFinalized)
		{
			connectionKey.ThrowIfNull("connectionKey");

			MySqlConnection connection = await _connectionProvider.GetConnectionAsync(connectionKey, true);
			MySqlTransaction transaction = connection.BeginTransaction(isolationLevel);

			return new ConnectionContext(connectionKey, connection, transaction, transactionDisposeBehavior);
		}
	}
}