using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Transactions;

using Junior.Common;

using IsolationLevel = System.Transactions.IsolationLevel;

namespace Junior.Data
{
	public class AsyncTransaction<TConnection> : IAsyncTransaction<TConnection>
		where TConnection : DbConnection
	{
		// ReSharper disable once StaticFieldInGenericType
		private static readonly object _lockObject = new object();
		private readonly IEnumerable<string> _connectionKeys;
		private readonly TransactionScope _transactionScope;
		private bool _disposed;

		public AsyncTransaction(IConnectionProvider<TConnection> connectionProvider, TransactionScopeOption transactionScopeOption, TransactionOptions transactionOptions, IEnumerable<string> connectionKeys)
		{
			connectionProvider.ThrowIfNull("connectionProvider");
			connectionKeys.ThrowIfNull("connectionKeys");

			_connectionKeys = connectionKeys.ToArray();

			lock (_lockObject)
			{
				string logicalDataName = GetLogicalDataName();
				var state = (AsyncTransactionLogicalState)CallContext.LogicalGetData(logicalDataName);

				if (state == null)
				{
					state = new AsyncTransactionLogicalState(connectionProvider);
					CallContext.LogicalSetData(logicalDataName, state);
				}

				state.Push(this);
			}

			_transactionScope = new TransactionScope(transactionScopeOption, transactionOptions, TransactionScopeAsyncFlowOption.Enabled);
		}

		public AsyncTransaction(IConnectionProvider<TConnection> connectionProvider, TransactionScopeOption transactionScopeOption, TransactionOptions transactionOptions, params string[] connectionKeys)
			: this(connectionProvider, transactionScopeOption, transactionOptions, (IEnumerable<string>)connectionKeys)
		{
		}

		public AsyncTransaction(IConnectionProvider<TConnection> connectionProvider, IsolationLevel isolationLevel, TimeSpan timeout, IEnumerable<string> connectionKeys)
			: this(connectionProvider, TransactionScopeOption.Required, new TransactionOptions { IsolationLevel = isolationLevel, Timeout = timeout }, connectionKeys)
		{
		}

		public AsyncTransaction(IConnectionProvider<TConnection> connectionProvider, IsolationLevel isolationLevel, TimeSpan timeout, params string[] connectionKeys)
			: this(connectionProvider, TransactionScopeOption.Required, new TransactionOptions { IsolationLevel = isolationLevel, Timeout = timeout }, (IEnumerable<string>)connectionKeys)
		{
		}

		public AsyncTransaction(IConnectionProvider<TConnection> connectionProvider, IsolationLevel isolationLevel, IEnumerable<string> connectionKeys)
			: this(connectionProvider, TransactionScopeOption.Required, new TransactionOptions { IsolationLevel = isolationLevel }, connectionKeys)
		{
		}

		public AsyncTransaction(IConnectionProvider<TConnection> connectionProvider, IsolationLevel isolationLevel, params string[] connectionKeys)
			: this(connectionProvider, TransactionScopeOption.Required, new TransactionOptions { IsolationLevel = isolationLevel }, (IEnumerable<string>)connectionKeys)
		{
		}

		public AsyncTransaction(IConnectionProvider<TConnection> connectionProvider, TimeSpan timeout, IEnumerable<string> connectionKeys)
			: this(connectionProvider, TransactionScopeOption.Required, new TransactionOptions { Timeout = timeout }, connectionKeys)
		{
		}

		public AsyncTransaction(IConnectionProvider<TConnection> connectionProvider, TimeSpan timeout, params string[] connectionKeys)
			: this(connectionProvider, TransactionScopeOption.Required, new TransactionOptions { Timeout = timeout }, (IEnumerable<string>)connectionKeys)
		{
		}

		public AsyncTransaction(IConnectionProvider<TConnection> connectionProvider, IEnumerable<string> connectionKeys)
			: this(connectionProvider, TransactionScopeOption.Required, new TransactionOptions(), connectionKeys)
		{
		}

		public AsyncTransaction(IConnectionProvider<TConnection> connectionProvider, params string[] connectionKeys)
			: this(connectionProvider, TransactionScopeOption.Required, new TransactionOptions(), (IEnumerable<string>)connectionKeys)
		{
		}

		public static AsyncTransaction<TConnection> Current
		{
			get
			{
				var state = (AsyncTransactionLogicalState)CallContext.LogicalGetData(GetLogicalDataName());

				return state.IfNotNull(arg => arg.CurrentTransaction);
			}
		}

		public TConnection GetConnection(string connectionKey, bool openConnection = true)
		{
			connectionKey.ThrowIfNull("connectionKey");

			var state = (AsyncTransactionLogicalState)CallContext.LogicalGetData(GetLogicalDataName());

			if (state == null)
			{
				throw new InvalidOperationException("No transaction is available in this context.");
			}

			return state.GetConnection(connectionKey, openConnection);
		}

		public void Commit()
		{
			this.ThrowIfDisposed(_disposed);

			_transactionScope.Complete();
		}

		public void Dispose()
		{
			OnDispose(true);
			GC.SuppressFinalize(this);
		}

		~AsyncTransaction()
		{
			OnDispose(false);
		}

		protected virtual void OnDispose(bool disposing)
		{
			if (!_disposed && disposing)
			{
				var state = (AsyncTransactionLogicalState)CallContext.LogicalGetData(GetLogicalDataName());

				if (state != null)
				{
					state.Pop(this);
				}

				try
				{
					_transactionScope.Dispose();
				}
				catch (TransactionAbortedException)
				{
				}
			}

			_disposed = true;
		}

		private static string GetLogicalDataName()
		{
			return typeof(AsyncTransaction<TConnection>).FullName;
		}

		private class AsyncTransactionLogicalState
		{
			private readonly IConnectionProvider<TConnection> _connectionProvider;
			private readonly ConcurrentDictionary<string, TConnection> _connectionsByConnectionKey = new ConcurrentDictionary<string, TConnection>();
			private readonly ConcurrentDictionary<string, int> _countsByConnectionKey = new ConcurrentDictionary<string, int>();
			private readonly ConcurrentStack<AsyncTransaction<TConnection>> _stack = new ConcurrentStack<AsyncTransaction<TConnection>>();

			public AsyncTransactionLogicalState(IConnectionProvider<TConnection> connectionProvider)
			{
				_connectionProvider = connectionProvider;
			}

			public AsyncTransaction<TConnection> CurrentTransaction
			{
				get
				{
					AsyncTransaction<TConnection> transaction;

					_stack.TryPeek(out transaction);

					return transaction;
				}
			}

			public void Push(AsyncTransaction<TConnection> transaction)
			{
				foreach (string connectionKey in transaction._connectionKeys)
				{
					_connectionsByConnectionKey.AddOrUpdate(connectionKey, key => _connectionProvider.GetConnection(key, false), (key, value) => value);
					_countsByConnectionKey.AddOrUpdate(connectionKey, key => 1, (key, value) => value + 1);
				}

				_stack.Push(transaction);
			}

			public void Pop(AsyncTransaction<TConnection> transaction)
			{
				foreach (string connectionKey in transaction._connectionKeys)
				{
					if (--_countsByConnectionKey[connectionKey] > 0)
					{
						continue;
					}

					TConnection connection;

					_connectionsByConnectionKey.TryRemove(connectionKey, out connection);

					connection.Dispose();
				}

				AsyncTransaction<TConnection> poppedTransaction;

				_stack.TryPop(out poppedTransaction);

				if (transaction != poppedTransaction)
				{
					throw new InvalidOperationException("Must dispose transactions in order of creation, reversed.");
				}
			}

			public TConnection GetConnection(string connectionKey, bool openConnection = true)
			{
				TConnection connection = _connectionsByConnectionKey[connectionKey];

				if (connection == null)
				{
					throw new InvalidOperationException(String.Format("No connection with the key '{0}' is available in this context.", connectionKey));
				}
				if (openConnection && connection.State == ConnectionState.Closed)
				{
					connection.Open();
				}

				return connection;
			}
		}
	}
}