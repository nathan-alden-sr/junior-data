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
		private static readonly Dictionary<string, int> _connectionKeyCounts = new Dictionary<string, int>();
		// ReSharper disable once StaticFieldInGenericType
		private static readonly object _lockObject = new object();
		private static readonly ConcurrentStack<AsyncTransaction<TConnection>> _stack = new ConcurrentStack<AsyncTransaction<TConnection>>();
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
				foreach (string connectionKey in _connectionKeys)
				{
					string logicalDataName = GetLogicalDataName(connectionKey);

					if (CallContext.LogicalGetData(logicalDataName) == null)
					{
						CallContext.LogicalSetData(logicalDataName, connectionProvider.GetConnection(connectionKey, false));
					}

					int count;

					if (!_connectionKeyCounts.TryGetValue(connectionKey, out count))
					{
						_connectionKeyCounts.Add(connectionKey, 1);
					}
					else
					{
						_connectionKeyCounts[connectionKey] = count + 1;
					}
				}
			}

			_transactionScope = new TransactionScope(transactionScopeOption, transactionOptions, TransactionScopeAsyncFlowOption.Enabled);
			_stack.Push(this);
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
				AsyncTransaction<TConnection> transaction;

				_stack.TryPeek(out transaction);

				return transaction;
			}
		}

		public TConnection GetConnection(string connectionKey, bool openConnection = true)
		{
			connectionKey.ThrowIfNull("connectionKey");

			string logicalDataName = GetLogicalDataName(connectionKey);

			lock (_lockObject)
			{
				var connection = (TConnection)CallContext.LogicalGetData(logicalDataName);

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
			if (!_disposed)
			{
				lock (_lockObject)
				{
					foreach (string connectionKey in _connectionKeys)
					{
						if (--_connectionKeyCounts[connectionKey] > 0)
						{
							continue;
						}

						string logicalDataName = GetLogicalDataName(connectionKey);
						var connection = (TConnection)CallContext.LogicalGetData(logicalDataName);

						connection.Dispose();
						CallContext.LogicalSetData(logicalDataName, null);
						_connectionKeyCounts.Remove(connectionKey);
					}

					AsyncTransaction<TConnection> transaction;

					_stack.TryPop(out transaction);
					try
					{
						_transactionScope.Dispose();
					}
					catch (TransactionAbortedException)
					{
					}
				}
			}

			_disposed = true;
		}

		private static string GetLogicalDataName(string connectionKey)
		{
			return String.Format("{0},{1}", typeof(AsyncTransaction<TConnection>).FullName, connectionKey);
		}
	}
}