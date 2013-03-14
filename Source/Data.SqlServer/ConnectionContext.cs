using System.Data.SqlClient;

namespace Junior.Data.SqlServer
{
	public class ConnectionContext : ConnectionContext<SqlConnection, SqlTransaction>
	{
		protected internal ConnectionContext(string connectionKey, SqlConnection connection, SqlTransaction transaction, TransactionDisposeBehavior transactionDisposeBehavior)
			: base(connectionKey, connection, transaction, transactionDisposeBehavior)
		{
		}
	}
}