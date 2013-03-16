using Junior.Common;

using MySql.Data.MySqlClient;

namespace Junior.Data.MySql
{
	public abstract class DataConnector : DataConnector<ConnectionContext, MySqlCommand, MySqlDataReader, MySqlDataAdapter, MySqlParameter, MySqlDbType>
	{
		protected DataConnector(ICommandProvider<ConnectionContext, MySqlCommand, MySqlParameter> commandProvider)
			: base(commandProvider)
		{
		}

		protected override sealed MySqlParameter GetParameter(string parameterName, object value)
		{
			parameterName.ThrowIfNull("parameterName");

			return new MySqlParameter(parameterName, value);
		}

		protected override sealed MySqlParameter GetParameter(string parameterName, object value, MySqlDbType type)
		{
			parameterName.ThrowIfNull("parameterName");

			return new MySqlParameter(parameterName, type) { Value = GetParameterValue(value) };
		}

		protected override sealed MySqlParameter GetParameter(string parameterName, object value, MySqlDbType type, int size)
		{
			parameterName.ThrowIfNull("parameterName");

			return new MySqlParameter(parameterName, type, size) { Value = GetParameterValue(value) };
		}

		protected override sealed MySqlParameter GetParameter(string parameterName, object value, MySqlDbType type, int size, byte precision, byte scale)
		{
			parameterName.ThrowIfNull("parameterName");

			return new MySqlParameter(parameterName, type, size)
				{
					Precision = precision,
					Scale = scale,
					Value = GetParameterValue(value)
				};
		}

		protected override sealed MySqlParameter GetParameter(string parameterName, object value, MySqlDbType type, byte precision, byte scale)
		{
			parameterName.ThrowIfNull("parameterName");

			return new MySqlParameter(parameterName, type)
				{
					Precision = precision,
					Scale = scale,
					Value = GetParameterValue(value)
				};
		}

		protected override sealed MySqlDataAdapter GetDataAdapter(MySqlCommand command)
		{
			return new MySqlDataAdapter(command);
		}
	}
}