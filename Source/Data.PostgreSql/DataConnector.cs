using Junior.Common;

using Npgsql;

using NpgsqlTypes;

namespace Junior.Data.PostgreSql
{
	public abstract class DataConnector : DataConnector<ConnectionContext, NpgsqlCommand, NpgsqlDataReader, NpgsqlDataAdapter, NpgsqlParameter, NpgsqlDbType>
	{
		protected DataConnector(ICommandProvider<ConnectionContext, NpgsqlCommand, NpgsqlParameter> commandProvider)
			: base(commandProvider)
		{
		}

		protected override sealed NpgsqlParameter GetParameter(string parameterName, object value)
		{
			parameterName.ThrowIfNull("parameterName");

			return new NpgsqlParameter(parameterName, value);
		}

		protected override sealed NpgsqlParameter GetParameter(string parameterName, object value, NpgsqlDbType type)
		{
			parameterName.ThrowIfNull("parameterName");

			return new NpgsqlParameter(parameterName, type) { Value = GetParameterValue(value) };
		}

		protected override sealed NpgsqlParameter GetParameter(string parameterName, object value, NpgsqlDbType type, int size)
		{
			parameterName.ThrowIfNull("parameterName");

			return new NpgsqlParameter(parameterName, type, size) { Value = GetParameterValue(value) };
		}

		protected override sealed NpgsqlParameter GetParameter(string parameterName, object value, NpgsqlDbType type, int size, byte precision, byte scale)
		{
			parameterName.ThrowIfNull("parameterName");

			return new NpgsqlParameter(parameterName, type, size)
				{
					Precision = precision,
					Scale = scale,
					Value = GetParameterValue(value)
				};
		}

		protected override sealed NpgsqlParameter GetParameter(string parameterName, object value, NpgsqlDbType type, byte precision, byte scale)
		{
			parameterName.ThrowIfNull("parameterName");

			return new NpgsqlParameter(parameterName, type)
				{
					Precision = precision,
					Scale = scale,
					Value = GetParameterValue(value)
				};
		}

		protected override sealed NpgsqlDataAdapter GetDataAdapter(NpgsqlCommand command)
		{
			return new NpgsqlDataAdapter(command);
		}
	}
}