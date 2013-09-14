using System;
using System.Data.Common;

namespace Junior.Data
{
	public interface IResolvedConnection<out TConnection> : IDisposable
		where TConnection : DbConnection
	{
		TConnection Connection
		{
			get;
		}
	}
}