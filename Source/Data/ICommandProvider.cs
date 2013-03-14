using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Junior.Data
{
	public interface ICommandProvider<in TConnectionContext, out TCommand, in TParameter>
		where TConnectionContext : class, IDisposable
		where TCommand : DbCommand
		where TParameter : DbParameter
	{
		TCommand GetCommand(TConnectionContext context, string sql, IEnumerable<TParameter> parameters);
		TCommand GetCommand(TConnectionContext context, string sql, params TParameter[] parameters);
	}
}