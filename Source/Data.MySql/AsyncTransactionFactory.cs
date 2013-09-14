using System;
using System.Collections.Generic;
using System.Transactions;

using Junior.Common;

using MySql.Data.MySqlClient;

namespace Junior.Data.MySql
{
	public class AsyncTransactionFactory : IAsyncTransactionFactory
	{
		private readonly IConnectionProvider<MySqlConnection> _connectionProvider;

		public AsyncTransactionFactory(IConnectionProvider<MySqlConnection> connectionProvider)
		{
			connectionProvider.ThrowIfNull("connectionProvider");

			_connectionProvider = connectionProvider;
		}

		public IAsyncTransaction Create(TransactionScopeOption transactionScopeOption, TransactionOptions transactionOptions, IEnumerable<string> connectionKeys)
		{
			return new AsyncTransaction<MySqlConnection>(_connectionProvider, transactionScopeOption, transactionOptions, connectionKeys);
		}

		public IAsyncTransaction Create(TransactionScopeOption transactionScopeOption, TransactionOptions transactionOptions, params string[] connectionKeys)
		{
			return new AsyncTransaction<MySqlConnection>(_connectionProvider, transactionScopeOption, transactionOptions, connectionKeys);
		}

		public IAsyncTransaction Create(IsolationLevel isolationLevel, TimeSpan timeout, IEnumerable<string> connectionKeys)
		{
			return new AsyncTransaction<MySqlConnection>(_connectionProvider, isolationLevel, timeout, connectionKeys);
		}

		public IAsyncTransaction Create(IsolationLevel isolationLevel, TimeSpan timeout, params string[] connectionKeys)
		{
			return new AsyncTransaction<MySqlConnection>(_connectionProvider, isolationLevel, timeout, connectionKeys);
		}

		public IAsyncTransaction Create(IsolationLevel isolationLevel, IEnumerable<string> connectionKeys)
		{
			return new AsyncTransaction<MySqlConnection>(_connectionProvider, isolationLevel, connectionKeys);
		}

		public IAsyncTransaction Create(IsolationLevel isolationLevel, params string[] connectionKeys)
		{
			return new AsyncTransaction<MySqlConnection>(_connectionProvider, isolationLevel, connectionKeys);
		}

		public IAsyncTransaction Create(TimeSpan timeout, IEnumerable<string> connectionKeys)
		{
			return new AsyncTransaction<MySqlConnection>(_connectionProvider, timeout, connectionKeys);
		}

		public IAsyncTransaction Create(TimeSpan timeout, params string[] connectionKeys)
		{
			return new AsyncTransaction<MySqlConnection>(_connectionProvider, timeout, connectionKeys);
		}

		public IAsyncTransaction Create(IEnumerable<string> connectionKeys)
		{
			return new AsyncTransaction<MySqlConnection>(_connectionProvider, connectionKeys);
		}

		public IAsyncTransaction Create(params string[] connectionKeys)
		{
			return new AsyncTransaction<MySqlConnection>(_connectionProvider, connectionKeys);
		}
	}
}