using System;
using System.Data.Common;

using Junior.Common;

namespace Junior.Data
{
	public abstract class ConnectionContext<TConnection, TTransaction> : IConnectionContext
		where TConnection : DbConnection
		where TTransaction : DbTransaction
	{
		private readonly string _connectionKey;
		private readonly TransactionDisposeBehavior _transactionDisposeBehavior;
		private bool _committed;
		private bool _rolledBack;

		protected ConnectionContext(string connectionKey, TConnection connection, TTransaction transaction, TransactionDisposeBehavior transactionDisposeBehavior)
		{
			connectionKey.ThrowIfNull("connectionKey");
			connection.ThrowIfNull("connection");

			_connectionKey = connectionKey;
			Connection = connection;
			Transaction = transaction;
			_transactionDisposeBehavior = transactionDisposeBehavior;
		}

		public string ConnectionKey
		{
			get
			{
				return _connectionKey;
			}
		}

		public TConnection Connection
		{
			get;
			private set;
		}

		public TTransaction Transaction
		{
			get;
			private set;
		}

		protected bool Disposed
		{
			get;
			private set;
		}

		public void Dispose()
		{
			this.ThrowIfDisposed(Disposed);

			OnDispose(true);
			GC.SuppressFinalize(this);
		}

		public void Commit()
		{
			this.ThrowIfDisposed(Disposed);

			Transaction.Commit();
			_committed = true;
		}

		public void Rollback()
		{
			this.ThrowIfDisposed(Disposed);

			Transaction.Rollback();
			_rolledBack = true;
		}

		protected virtual void OnDispose(bool disposing)
		{
			this.ThrowIfDisposed(Disposed);

			if (disposing)
			{
				if (Transaction != null)
				{
					switch (_transactionDisposeBehavior)
					{
						case TransactionDisposeBehavior.CommitIfNonFinalized:
							if (!_committed && !_rolledBack)
							{
								Commit();
							}
							break;
						case TransactionDisposeBehavior.RollbackIfNonFinalized:
							if (!_committed && !_rolledBack)
							{
								Rollback();
							}
							break;
					}
					Transaction.Dispose();
					Transaction = null;
				}
				if (Connection != null)
				{
					Connection.Dispose();
					Connection = null;
				}
			}

			Disposed = true;
		}
	}
}