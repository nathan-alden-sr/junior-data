using System;

namespace Junior.Data
{
	public interface IConnectionContext : IDisposable
	{
		void Commit();
		void Rollback();
	}
}