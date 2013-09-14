using System;
using System.Data.Common;

namespace Junior.Data
{
	public interface IAsyncTransaction : IDisposable
	{
		void Commit();
	}

	public interface IAsyncTransaction<out TConnection> : IAsyncTransaction
		where TConnection : DbConnection
	{
		TConnection GetConnection(string connectionKey, bool openConnection = true);
	}
}