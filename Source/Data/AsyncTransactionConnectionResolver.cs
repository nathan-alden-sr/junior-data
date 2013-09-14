using System;
using System.Data.Common;
using System.Threading.Tasks;

using Junior.Common;

namespace Junior.Data
{
	public class AsyncTransactionConnectionResolver<TConnection> : IConnectionResolver<TConnection>
		where TConnection : DbConnection
	{
		private readonly IConnectionProvider<TConnection> _connectionProvider;

		public AsyncTransactionConnectionResolver(IConnectionProvider<TConnection> connectionProvider)
		{
			connectionProvider.ThrowIfNull("connectionProvider");

			_connectionProvider = connectionProvider;
		}

		public async Task<IResolvedConnection<TConnection>> ResolveConnectionAsync(string connectionKey, bool openConnection = true)
		{
			AsyncTransaction<TConnection> asyncTransaction = AsyncTransaction<TConnection>.Current;

			return asyncTransaction != null
				? (IResolvedConnection<TConnection>)new ResolvedConnection(asyncTransaction.GetConnection(connectionKey, openConnection))
				: new ResolvedConnection(await _connectionProvider.GetConnectionAsync(connectionKey, openConnection), connection => connection.Dispose());
		}

		private class ResolvedConnection : IResolvedConnection<TConnection>
		{
			private readonly Action<TConnection> _disposingDelegate;
			private TConnection _connection;
			private bool _disposed;

			public ResolvedConnection(TConnection connection, Action<TConnection> disposingDelegate = null)
			{
				_connection = connection;
				_disposingDelegate = disposingDelegate;
			}

			public TConnection Connection
			{
				get
				{
					this.ThrowIfDisposed(_disposed);

					return _connection;
				}
			}

			public void Dispose()
			{
				if (_disposed)
				{
					return;
				}
				if (_disposingDelegate != null)
				{
					_disposingDelegate(_connection);
				}
				_connection = null;
				_disposed = true;
			}
		}
	}
}