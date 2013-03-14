using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

using Junior.Common;

namespace Junior.Data.SqlServer
{
	public class ConnectionContextFactory : IConnectionContextFactory<ConnectionContext>
	{
		private readonly IConnectionProvider<SqlConnection> _connectionProvider;

		public ConnectionContextFactory(IConnectionProvider<SqlConnection> connectionProvider)
		{
			connectionProvider.ThrowIfNull("connectionProvider");

			_connectionProvider = connectionProvider;
		}

		public async Task<ConnectionContext> Create(string connectionKey)
		{
			connectionKey.ThrowIfNull("connectionKey");

			SqlConnection connection = await _connectionProvider.GetConnection(connectionKey, false);

			return new ConnectionContext(connectionKey, connection, null, TransactionDisposeBehavior.None);
		}

		public async Task<ConnectionContext> CreateWithTransaction(string connectionKey, TransactionDisposeBehavior transactionDisposeBehavior = TransactionDisposeBehavior.RollbackIfNonFinalized)
		{
			connectionKey.ThrowIfNull("connectionKey");

			SqlConnection connection = await _connectionProvider.GetConnection(connectionKey, true);
			SqlTransaction transaction = connection.BeginTransaction();

			return new ConnectionContext(connectionKey, connection, transaction, transactionDisposeBehavior);
		}

		public async Task<ConnectionContext> CreateWithTransaction(
			string connectionKey,
			IsolationLevel isolationLevel,
			TransactionDisposeBehavior transactionDisposeBehavior = TransactionDisposeBehavior.RollbackIfNonFinalized)
		{
			connectionKey.ThrowIfNull("connectionKey");

			SqlConnection connection = await _connectionProvider.GetConnection(connectionKey, true);
			SqlTransaction transaction = connection.BeginTransaction(isolationLevel);

			return new ConnectionContext(connectionKey, connection, transaction, transactionDisposeBehavior);
		}
	}
}