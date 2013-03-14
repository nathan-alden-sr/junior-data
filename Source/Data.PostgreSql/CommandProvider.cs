using System.Collections.Generic;
using System.Data;
using System.Linq;

using Junior.Common;

using Npgsql;

namespace Junior.Data.PostgreSql
{
	public class CommandProvider : ICommandProvider<ConnectionContext, NpgsqlCommand, NpgsqlParameter>
	{
		private readonly ICommandTimeoutProvider _commandTimeoutProvider;

		public CommandProvider(ICommandTimeoutProvider commandTimeoutProvider)
		{
			commandTimeoutProvider.ThrowIfNull("commandTimeoutProvider");

			_commandTimeoutProvider = commandTimeoutProvider;
		}

		public NpgsqlCommand GetCommand(ConnectionContext context, string sql, IEnumerable<NpgsqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<NpgsqlParameter>();
			NpgsqlCommand command = context.Transaction == null ? new NpgsqlCommand(sql, context.Connection) : new NpgsqlCommand(sql, context.Connection, context.Transaction);

			command.CommandTimeout = (int)_commandTimeoutProvider.GetTimeout(context.ConnectionKey).TotalSeconds;
			command.CommandType = CommandType.Text;

			foreach (NpgsqlParameter parameter in parameters)
			{
				command.Parameters.Add(parameter);
			}
			command.Prepare();

			return command;
		}

		public NpgsqlCommand GetCommand(ConnectionContext context, string sql, params NpgsqlParameter[] parameters)
		{
			return GetCommand(context, sql, (IEnumerable<NpgsqlParameter>)parameters);
		}
	}
}