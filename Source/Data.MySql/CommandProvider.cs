using System.Collections.Generic;
using System.Data;
using System.Linq;

using Junior.Common;

using MySql.Data.MySqlClient;

namespace Junior.Data.MySql
{
	public class CommandProvider : ICommandProvider<ConnectionContext, MySqlCommand, MySqlParameter>
	{
		private readonly ICommandTimeoutProvider _commandTimeoutProvider;

		public CommandProvider(ICommandTimeoutProvider commandTimeoutProvider)
		{
			commandTimeoutProvider.ThrowIfNull("commandTimeoutProvider");

			_commandTimeoutProvider = commandTimeoutProvider;
		}

		public MySqlCommand GetCommand(ConnectionContext context, string sql, IEnumerable<MySqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<MySqlParameter>();
			MySqlCommand command = context.Transaction == null ? new MySqlCommand(sql, context.Connection) : new MySqlCommand(sql, context.Connection, context.Transaction);

			command.CommandTimeout = (int)_commandTimeoutProvider.GetTimeout(context.ConnectionKey).TotalSeconds;
			command.CommandType = CommandType.Text;

			foreach (MySqlParameter parameter in parameters)
			{
				command.Parameters.Add(parameter);
			}

			return command;
		}

		public MySqlCommand GetCommand(ConnectionContext context, string sql, params MySqlParameter[] parameters)
		{
			return GetCommand(context, sql, (IEnumerable<MySqlParameter>)parameters);
		}
	}
}