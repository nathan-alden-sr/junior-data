using System;
using System.Collections.Generic;
using System.Transactions;

using Junior.Common;

using Npgsql;

namespace Junior.Data.PostgreSql
{
	public class AsyncTransactionFactory : IAsyncTransactionFactory
	{
		private readonly IConnectionProvider<NpgsqlConnection> _connectionProvider;

		public AsyncTransactionFactory(IConnectionProvider<NpgsqlConnection> connectionProvider)
		{
			connectionProvider.ThrowIfNull("connectionProvider");

			_connectionProvider = connectionProvider;
		}

		public IAsyncTransaction Create(TransactionScopeOption transactionScopeOption, TransactionOptions transactionOptions, IEnumerable<string> connectionKeys)
		{
			return new AsyncTransaction<NpgsqlConnection>(_connectionProvider, transactionScopeOption, transactionOptions, connectionKeys);
		}

		public IAsyncTransaction Create(TransactionScopeOption transactionScopeOption, TransactionOptions transactionOptions, params string[] connectionKeys)
		{
			return new AsyncTransaction<NpgsqlConnection>(_connectionProvider, transactionScopeOption, transactionOptions, connectionKeys);
		}

		public IAsyncTransaction Create(IsolationLevel isolationLevel, TimeSpan timeout, IEnumerable<string> connectionKeys)
		{
			return new AsyncTransaction<NpgsqlConnection>(_connectionProvider, isolationLevel, timeout, connectionKeys);
		}

		public IAsyncTransaction Create(IsolationLevel isolationLevel, TimeSpan timeout, params string[] connectionKeys)
		{
			return new AsyncTransaction<NpgsqlConnection>(_connectionProvider, isolationLevel, timeout, connectionKeys);
		}

		public IAsyncTransaction Create(IsolationLevel isolationLevel, IEnumerable<string> connectionKeys)
		{
			return new AsyncTransaction<NpgsqlConnection>(_connectionProvider, isolationLevel, connectionKeys);
		}

		public IAsyncTransaction Create(IsolationLevel isolationLevel, params string[] connectionKeys)
		{
			return new AsyncTransaction<NpgsqlConnection>(_connectionProvider, isolationLevel, connectionKeys);
		}

		public IAsyncTransaction Create(TimeSpan timeout, IEnumerable<string> connectionKeys)
		{
			return new AsyncTransaction<NpgsqlConnection>(_connectionProvider, timeout, connectionKeys);
		}

		public IAsyncTransaction Create(TimeSpan timeout, params string[] connectionKeys)
		{
			return new AsyncTransaction<NpgsqlConnection>(_connectionProvider, timeout, connectionKeys);
		}

		public IAsyncTransaction Create(IEnumerable<string> connectionKeys)
		{
			return new AsyncTransaction<NpgsqlConnection>(_connectionProvider, connectionKeys);
		}

		public IAsyncTransaction Create(params string[] connectionKeys)
		{
			return new AsyncTransaction<NpgsqlConnection>(_connectionProvider, connectionKeys);
		}
	}
}