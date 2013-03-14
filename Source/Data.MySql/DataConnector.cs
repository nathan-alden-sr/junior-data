using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Junior.Common;

using MySql.Data.MySqlClient;

namespace Junior.Data.MySql
{
	public abstract class DataConnector
	{
		private readonly ICommandProvider<ConnectionContext, MySqlCommand, MySqlParameter> _commandProvider;

		protected DataConnector(ICommandProvider<ConnectionContext, MySqlCommand, MySqlParameter> commandProvider)
		{
			commandProvider.ThrowIfNull("commandProvider");

			_commandProvider = commandProvider;
		}

		protected MySqlParameter GetParameter(string parameterName, object value)
		{
			parameterName.ThrowIfNull("parameterName");

			return new MySqlParameter(parameterName, value);
		}

		protected MySqlParameter GetParameter(string parameterName, object value, MySqlDbType type)
		{
			parameterName.ThrowIfNull("parameterName");

			return new MySqlParameter(parameterName, type) { Value = GetSqlParameterValue(value) };
		}

		protected MySqlParameter GetParameter(string parameterName, object value, MySqlDbType type, int size)
		{
			parameterName.ThrowIfNull("parameterName");

			return new MySqlParameter(parameterName, type, size) { Value = GetSqlParameterValue(value) };
		}

		protected MySqlParameter GetParameter(string parameterName, object value, MySqlDbType type, int size, byte precision, byte scale)
		{
			parameterName.ThrowIfNull("parameterName");

			return new MySqlParameter(parameterName, type, size)
				{
					Precision = precision,
					Scale = scale,
					Value = GetSqlParameterValue(value)
				};
		}

		protected MySqlParameter GetParameter(string parameterName, object value, MySqlDbType type, byte precision, byte scale)
		{
			parameterName.ThrowIfNull("parameterName");

			return new MySqlParameter(parameterName, type)
				{
					Precision = precision,
					Scale = scale,
					Value = GetSqlParameterValue(value)
				};
		}

		protected async Task<int> ExecuteNonQuery(ConnectionContext context, string sql, IEnumerable<MySqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<MySqlParameter>();

			string formattedSql = FormatSql(sql);
			MySqlCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);

			return await command.ExecuteNonQueryAsync();
		}

		protected async Task<int> ExecuteNonQuery(ConnectionContext context, string sql, params MySqlParameter[] parameters)
		{
			return await ExecuteNonQuery(context, sql, (IEnumerable<MySqlParameter>)parameters);
		}

		protected async Task<T> ExecuteScalar<T>(ConnectionContext context, string sql, IEnumerable<MySqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<MySqlParameter>();

			string formattedSql = FormatSql(sql);
			MySqlCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);
			object value = await command.ExecuteScalarAsync();

			if (value == null || value == DBNull.Value)
			{
				Type type = typeof(T);

				if (type.IsValueType && (!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(Nullable<>)))
				{
					throw new Exception("Query resulted in NULL but scalar type is a non-nullable value type.");
				}

				return default(T);
			}

			return (T)value;
		}

		protected async Task<T> ExecuteScalar<T>(ConnectionContext context, string sql, params MySqlParameter[] parameters)
		{
			return await ExecuteScalar<T>(context, sql, (IEnumerable<MySqlParameter>)parameters);
		}

		protected async Task<MySqlDataReader> ExecuteReader(ConnectionContext context, CommandBehavior commandBehavior, string sql, IEnumerable<MySqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<MySqlParameter>();

			string formattedSql = FormatSql(sql);
			MySqlCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);

			return (MySqlDataReader)await command.ExecuteReaderAsync(commandBehavior);
		}

		protected async Task<MySqlDataReader> ExecuteReader(ConnectionContext context, CommandBehavior commandBehavior, string sql, params MySqlParameter[] parameters)
		{
			return await ExecuteReader(context, commandBehavior, sql, (IEnumerable<MySqlParameter>)parameters);
		}

		protected async Task<MySqlDataReader> ExecuteReader(ConnectionContext context, string sql, IEnumerable<MySqlParameter> parameters)
		{
			return await ExecuteReader(context, CommandBehavior.Default, sql, parameters);
		}

		protected async Task<MySqlDataReader> ExecuteReader(ConnectionContext context, string sql, params MySqlParameter[] parameters)
		{
			return await ExecuteReader(context, CommandBehavior.Default, sql, (IEnumerable<MySqlParameter>)parameters);
		}

		protected async Task<IEnumerable<T>> ExecuteProjection<T>(ConnectionContext context, string sql, Func<MySqlDataReader, T> getProjectedObjectDelegate, IEnumerable<MySqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");
			getProjectedObjectDelegate.ThrowIfNull("getProjectedObjectDelegate");

			parameters = parameters ?? Enumerable.Empty<MySqlParameter>();

			var projections = new List<T>();

			using (MySqlDataReader reader = await ExecuteReader(context, sql, parameters))
			{
				if (reader.HasRows)
				{
					while (reader.Read())
					{
						projections.Add(getProjectedObjectDelegate(reader));
					}
				}
			}

			return projections;
		}

		protected async Task<IEnumerable<T>> ExecuteProjection<T>(ConnectionContext context, string sql, Func<MySqlDataReader, T> getProjectedObjectDelegate, params MySqlParameter[] parameters)
		{
			return await ExecuteProjection(context, sql, getProjectedObjectDelegate, (IEnumerable<MySqlParameter>)parameters);
		}

		protected IEnumerable<T> ExecuteProjection<T>(ConnectionContext context, string sql, Func<DataRow, T> getProjectedObjectDelegate, IEnumerable<MySqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");
			getProjectedObjectDelegate.ThrowIfNull("getProjectedObjectDelegate");

			parameters = parameters ?? Enumerable.Empty<MySqlParameter>();

			var table = new DataTable();
			string formattedSql = FormatSql(sql);
			MySqlCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);
			var dataAdapter = new MySqlDataAdapter(command);

			dataAdapter.Fill(table);

			return table.Rows.Cast<DataRow>().Select(getProjectedObjectDelegate);
		}

		protected IEnumerable<T> ExecuteProjection<T>(ConnectionContext context, string sql, Func<DataRow, T> getProjectedObjectDelegate, params MySqlParameter[] parameters)
		{
			return ExecuteProjection(context, sql, getProjectedObjectDelegate, (IEnumerable<MySqlParameter>)parameters);
		}

		private static string FormatSql(string sql)
		{
			return sql.Trim();
		}

		private static object GetSqlParameterValue(object value)
		{
			return value ?? DBNull.Value;
		}
	}
}