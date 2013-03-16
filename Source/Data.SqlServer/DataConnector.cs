using System.Data;
using System.Data.SqlClient;

using Junior.Common;

namespace Junior.Data.SqlServer
{
	public abstract class DataConnector : DataConnector<ConnectionContext, SqlCommand, SqlDataReader, SqlDataAdapter, SqlParameter, SqlDbType>
	{
		protected DataConnector(ICommandProvider<ConnectionContext, SqlCommand, SqlParameter> commandProvider)
			: base(commandProvider)
		{
		}

		protected override sealed SqlParameter GetParameter(string parameterName, object value)
		{
			parameterName.ThrowIfNull("parameterName");

			return new SqlParameter(parameterName, value);
		}

		protected override sealed SqlParameter GetParameter(string parameterName, object value, SqlDbType type)
		{
			parameterName.ThrowIfNull("parameterName");

			return new SqlParameter(parameterName, type) { Value = GetParameterValue(value) };
		}

		protected override sealed SqlParameter GetParameter(string parameterName, object value, SqlDbType type, int size)
		{
			parameterName.ThrowIfNull("parameterName");

			return new SqlParameter(parameterName, type, size) { Value = GetParameterValue(value) };
		}

		protected override sealed SqlParameter GetParameter(string parameterName, object value, SqlDbType type, int size, byte precision, byte scale)
		{
			parameterName.ThrowIfNull("parameterName");

			return new SqlParameter(parameterName, type, size)
				{
					Precision = precision,
					Scale = scale,
					Value = GetParameterValue(value)
				};
		}

		protected override sealed SqlParameter GetParameter(string parameterName, object value, SqlDbType type, byte precision, byte scale)
		{
			parameterName.ThrowIfNull("parameterName");

			return new SqlParameter(parameterName, type)
				{
					Precision = precision,
					Scale = scale,
					Value = GetParameterValue(value)
				};
		}

		protected override sealed SqlDataAdapter GetDataAdapter(SqlCommand command)
		{
			return new SqlDataAdapter(command);
		}
	}
}