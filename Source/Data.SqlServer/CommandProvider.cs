using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

using Junior.Common;

namespace Junior.Data.SqlServer
{
	public class CommandProvider : ICommandProvider<ConnectionContext, SqlCommand, SqlParameter>
	{
		private readonly ICommandTimeoutProvider _commandTimeoutProvider;

		public CommandProvider(ICommandTimeoutProvider commandTimeoutProvider)
		{
			commandTimeoutProvider.ThrowIfNull("commandTimeoutProvider");

			_commandTimeoutProvider = commandTimeoutProvider;
		}

		public SqlCommand GetCommand(ConnectionContext context, string sql, IEnumerable<SqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<SqlParameter>();
			SqlCommand command = context.Transaction == null ? new SqlCommand(sql, context.Connection) : new SqlCommand(sql, context.Connection, context.Transaction);

			command.CommandTimeout = (int)_commandTimeoutProvider.GetTimeout(context.ConnectionKey).TotalSeconds;
			command.CommandType = CommandType.Text;

			foreach (SqlParameter parameter in parameters)
			{
				command.Parameters.Add(parameter);
			}

			return command;
		}

		public SqlCommand GetCommand(ConnectionContext context, string sql, params SqlParameter[] parameters)
		{
			return GetCommand(context, sql, (IEnumerable<SqlParameter>)parameters);
		}
	}
}