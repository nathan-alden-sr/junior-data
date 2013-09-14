using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Transactions;

using Junior.Common;

namespace Junior.Data.SqlServer
{
	public class AsyncTransactionFactory : IAsyncTransactionFactory
	{
		private readonly IConnectionProvider<SqlConnection> _connectionProvider;

		public AsyncTransactionFactory(IConnectionProvider<SqlConnection> connectionProvider)
		{
			connectionProvider.ThrowIfNull("connectionProvider");

			_connectionProvider = connectionProvider;
		}

		public IAsyncTransaction Create(TransactionScopeOption transactionScopeOption, TransactionOptions transactionOptions, IEnumerable<string> connectionKeys)
		{
			return new AsyncTransaction<SqlConnection>(_connectionProvider, transactionScopeOption, transactionOptions, connectionKeys);
		}

		public IAsyncTransaction Create(TransactionScopeOption transactionScopeOption, TransactionOptions transactionOptions, params string[] connectionKeys)
		{
			return new AsyncTransaction<SqlConnection>(_connectionProvider, transactionScopeOption, transactionOptions, connectionKeys);
		}

		public IAsyncTransaction Create(IsolationLevel isolationLevel, TimeSpan timeout, IEnumerable<string> connectionKeys)
		{
			return new AsyncTransaction<SqlConnection>(_connectionProvider, isolationLevel, timeout, connectionKeys);
		}

		public IAsyncTransaction Create(IsolationLevel isolationLevel, TimeSpan timeout, params string[] connectionKeys)
		{
			return new AsyncTransaction<SqlConnection>(_connectionProvider, isolationLevel, timeout, connectionKeys);
		}

		public IAsyncTransaction Create(IsolationLevel isolationLevel, IEnumerable<string> connectionKeys)
		{
			return new AsyncTransaction<SqlConnection>(_connectionProvider, isolationLevel, connectionKeys);
		}

		public IAsyncTransaction Create(IsolationLevel isolationLevel, params string[] connectionKeys)
		{
			return new AsyncTransaction<SqlConnection>(_connectionProvider, isolationLevel, connectionKeys);
		}

		public IAsyncTransaction Create(TimeSpan timeout, IEnumerable<string> connectionKeys)
		{
			return new AsyncTransaction<SqlConnection>(_connectionProvider, timeout, connectionKeys);
		}

		public IAsyncTransaction Create(TimeSpan timeout, params string[] connectionKeys)
		{
			return new AsyncTransaction<SqlConnection>(_connectionProvider, timeout, connectionKeys);
		}

		public IAsyncTransaction Create(IEnumerable<string> connectionKeys)
		{
			return new AsyncTransaction<SqlConnection>(_connectionProvider, connectionKeys);
		}

		public IAsyncTransaction Create(params string[] connectionKeys)
		{
			return new AsyncTransaction<SqlConnection>(_connectionProvider, connectionKeys);
		}
	}
}