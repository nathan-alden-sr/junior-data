using Junior.Common;

using Npgsql;

namespace Junior.Data.PostgreSql
{
	public class ConnectionContext : ConnectionContext<NpgsqlConnection, NpgsqlTransaction>
	{
		protected internal ConnectionContext(string connectionKey, NpgsqlConnection connection, NpgsqlTransaction transaction, TransactionDisposeBehavior transactionDisposeBehavior)
			: base(connectionKey, connection, transaction, transactionDisposeBehavior)
		{
		}

		public void Save(string savePointName)
		{
			savePointName.ThrowIfNull("savePointName");

			Transaction.Save(savePointName);
		}

		public void Rollback(string savePointName)
		{
			savePointName.ThrowIfNull("savePointName");

			Transaction.Rollback(savePointName);
		}
	}
}