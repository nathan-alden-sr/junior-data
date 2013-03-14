using MySql.Data.MySqlClient;

namespace Junior.Data.MySql
{
	public class ConnectionContext : ConnectionContext<MySqlConnection, MySqlTransaction>
	{
		protected internal ConnectionContext(string connectionKey, MySqlConnection connection, MySqlTransaction transaction, TransactionDisposeBehavior transactionDisposeBehavior)
			: base(connectionKey, connection, transaction, transactionDisposeBehavior)
		{
		}
	}
}