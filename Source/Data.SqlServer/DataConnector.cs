using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

using Junior.Common;

namespace Junior.Data.SqlServer
{
	public abstract class DataConnector
	{
		private readonly ICommandProvider<ConnectionContext, SqlCommand, SqlParameter> _commandProvider;

		protected DataConnector(ICommandProvider<ConnectionContext, SqlCommand, SqlParameter> commandProvider)
		{
			commandProvider.ThrowIfNull("commandProvider");

			_commandProvider = commandProvider;
		}

		protected SqlParameter GetParameter(string parameterName, object value)
		{
			parameterName.ThrowIfNull("parameterName");

			return new SqlParameter(parameterName, value);
		}

		protected SqlParameter GetParameter(string parameterName, object value, SqlDbType type)
		{
			parameterName.ThrowIfNull("parameterName");

			return new SqlParameter(parameterName, type) { Value = GetSqlParameterValue(value) };
		}

		protected SqlParameter GetParameter(string parameterName, object value, SqlDbType type, int size)
		{
			parameterName.ThrowIfNull("parameterName");

			return new SqlParameter(parameterName, type, size) { Value = GetSqlParameterValue(value) };
		}

		protected SqlParameter GetParameter(string parameterName, object value, SqlDbType type, int size, byte precision, byte scale)
		{
			parameterName.ThrowIfNull("parameterName");

			return new SqlParameter(parameterName, type, size)
				{
					Precision = precision,
					Scale = scale,
					Value = GetSqlParameterValue(value)
				};
		}

		protected SqlParameter GetParameter(string parameterName, object value, SqlDbType type, byte precision, byte scale)
		{
			parameterName.ThrowIfNull("parameterName");

			return new SqlParameter(parameterName, type)
				{
					Precision = precision,
					Scale = scale,
					Value = GetSqlParameterValue(value)
				};
		}

		protected async Task<int> ExecuteNonQuery(ConnectionContext context, string sql, IEnumerable<SqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<SqlParameter>();

			string formattedSql = FormatSql(sql);
			SqlCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);

			return await command.ExecuteNonQueryAsync();
		}

		protected async Task<int> ExecuteNonQuery(ConnectionContext context, string sql, params SqlParameter[] parameters)
		{
			return await ExecuteNonQuery(context, sql, (IEnumerable<SqlParameter>)parameters);
		}

		protected async Task<T> ExecuteScalar<T>(ConnectionContext context, string sql, IEnumerable<SqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<SqlParameter>();

			string formattedSql = FormatSql(sql);
			SqlCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);
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

		protected async Task<T> ExecuteScalar<T>(ConnectionContext context, string sql, params SqlParameter[] parameters)
		{
			return await ExecuteScalar<T>(context, sql, (IEnumerable<SqlParameter>)parameters);
		}

		protected async Task<SqlDataReader> ExecuteReader(ConnectionContext context, CommandBehavior commandBehavior, string sql, IEnumerable<SqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<SqlParameter>();

			string formattedSql = FormatSql(sql);
			SqlCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);

			return await command.ExecuteReaderAsync(commandBehavior);
		}

		protected async Task<SqlDataReader> ExecuteReader(ConnectionContext context, CommandBehavior commandBehavior, string sql, params SqlParameter[] parameters)
		{
			return await ExecuteReader(context, commandBehavior, sql, (IEnumerable<SqlParameter>)parameters);
		}

		protected async Task<SqlDataReader> ExecuteReader(ConnectionContext context, string sql, IEnumerable<SqlParameter> parameters)
		{
			return await ExecuteReader(context, CommandBehavior.Default, sql, parameters);
		}

		protected async Task<SqlDataReader> ExecuteReader(ConnectionContext context, string sql, params SqlParameter[] parameters)
		{
			return await ExecuteReader(context, CommandBehavior.Default, sql, (IEnumerable<SqlParameter>)parameters);
		}

		protected async Task<IEnumerable<T>> ExecuteProjection<T>(ConnectionContext context, string sql, Func<SqlDataReader, T> getProjectedObjectDelegate, IEnumerable<SqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");
			getProjectedObjectDelegate.ThrowIfNull("getProjectedObjectDelegate");

			parameters = parameters ?? Enumerable.Empty<SqlParameter>();

			var projections = new List<T>();

			using (SqlDataReader reader = await ExecuteReader(context, sql, parameters))
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

		protected async Task<IEnumerable<T>> ExecuteProjection<T>(ConnectionContext context, string sql, Func<SqlDataReader, T> getProjectedObjectDelegate, params SqlParameter[] parameters)
		{
			return await ExecuteProjection(context, sql, getProjectedObjectDelegate, (IEnumerable<SqlParameter>)parameters);
		}

		protected IEnumerable<T> ExecuteProjection<T>(ConnectionContext context, string sql, Func<DataRow, T> getProjectedObjectDelegate, IEnumerable<SqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");
			getProjectedObjectDelegate.ThrowIfNull("getProjectedObjectDelegate");

			parameters = parameters ?? Enumerable.Empty<SqlParameter>();

			var table = new DataTable();
			string formattedSql = FormatSql(sql);
			SqlCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);
			var dataAdapter = new SqlDataAdapter(command);

			dataAdapter.Fill(table);

			return table.Rows.Cast<DataRow>().Select(getProjectedObjectDelegate);
		}

		protected IEnumerable<T> ExecuteProjection<T>(ConnectionContext context, string sql, Func<DataRow, T> getProjectedObjectDelegate, params SqlParameter[] parameters)
		{
			return ExecuteProjection(context, sql, getProjectedObjectDelegate, (IEnumerable<SqlParameter>)parameters);
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