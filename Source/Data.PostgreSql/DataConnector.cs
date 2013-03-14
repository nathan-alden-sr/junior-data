using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Junior.Common;

using Npgsql;

using NpgsqlTypes;

namespace Junior.Data.PostgreSql
{
	public abstract class DataConnector
	{
		private readonly ICommandProvider<ConnectionContext, NpgsqlCommand, NpgsqlParameter> _commandProvider;

		protected DataConnector(ICommandProvider<ConnectionContext, NpgsqlCommand, NpgsqlParameter> commandProvider)
		{
			commandProvider.ThrowIfNull("commandProvider");

			_commandProvider = commandProvider;
		}

		protected NpgsqlParameter GetParameter(string parameterName, object value)
		{
			parameterName.ThrowIfNull("parameterName");

			return new NpgsqlParameter(parameterName, value);
		}

		protected NpgsqlParameter GetParameter(string parameterName, object value, NpgsqlDbType type)
		{
			parameterName.ThrowIfNull("parameterName");

			return new NpgsqlParameter(parameterName, type) { Value = GetSqlParameterValue(value) };
		}

		protected NpgsqlParameter GetParameter(string parameterName, object value, NpgsqlDbType type, int size)
		{
			parameterName.ThrowIfNull("parameterName");

			return new NpgsqlParameter(parameterName, type, size) { Value = GetSqlParameterValue(value) };
		}

		protected NpgsqlParameter GetParameter(string parameterName, object value, NpgsqlDbType type, int size, byte precision, byte scale)
		{
			parameterName.ThrowIfNull("parameterName");

			return new NpgsqlParameter(parameterName, type, size)
				{
					Precision = precision,
					Scale = scale,
					Value = GetSqlParameterValue(value)
				};
		}

		protected NpgsqlParameter GetParameter(string parameterName, object value, NpgsqlDbType type, byte precision, byte scale)
		{
			parameterName.ThrowIfNull("parameterName");

			return new NpgsqlParameter(parameterName, type)
				{
					Precision = precision,
					Scale = scale,
					Value = GetSqlParameterValue(value)
				};
		}

		protected async Task<int> ExecuteNonQuery(ConnectionContext context, string sql, IEnumerable<NpgsqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<NpgsqlParameter>();

			string formattedSql = FormatSql(sql);
			NpgsqlCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);

			return await command.ExecuteNonQueryAsync();
		}

		protected async Task<int> ExecuteNonQuery(ConnectionContext context, string sql, params NpgsqlParameter[] parameters)
		{
			return await ExecuteNonQuery(context, sql, (IEnumerable<NpgsqlParameter>)parameters);
		}

		protected async Task<T> ExecuteScalar<T>(ConnectionContext context, string sql, IEnumerable<NpgsqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<NpgsqlParameter>();

			string formattedSql = FormatSql(sql);
			NpgsqlCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);
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

		protected async Task<T> ExecuteScalar<T>(ConnectionContext context, string sql, params NpgsqlParameter[] parameters)
		{
			return await ExecuteScalar<T>(context, sql, (IEnumerable<NpgsqlParameter>)parameters);
		}

		protected async Task<NpgsqlDataReader> ExecuteReader(ConnectionContext context, CommandBehavior commandBehavior, string sql, IEnumerable<NpgsqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<NpgsqlParameter>();

			string formattedSql = FormatSql(sql);
			NpgsqlCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);

			return (NpgsqlDataReader)await command.ExecuteReaderAsync(commandBehavior);
		}

		protected async Task<NpgsqlDataReader> ExecuteReader(ConnectionContext context, CommandBehavior commandBehavior, string sql, params NpgsqlParameter[] parameters)
		{
			return await ExecuteReader(context, commandBehavior, sql, (IEnumerable<NpgsqlParameter>)parameters);
		}

		protected async Task<NpgsqlDataReader> ExecuteReader(ConnectionContext context, string sql, IEnumerable<NpgsqlParameter> parameters)
		{
			return await ExecuteReader(context, CommandBehavior.Default, sql, parameters);
		}

		protected async Task<NpgsqlDataReader> ExecuteReader(ConnectionContext context, string sql, params NpgsqlParameter[] parameters)
		{
			return await ExecuteReader(context, CommandBehavior.Default, sql, (IEnumerable<NpgsqlParameter>)parameters);
		}

		protected async Task<IEnumerable<T>> ExecuteProjection<T>(ConnectionContext context, string sql, Func<NpgsqlDataReader, T> getProjectedObjectDelegate, IEnumerable<NpgsqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");
			getProjectedObjectDelegate.ThrowIfNull("getProjectedObjectDelegate");

			parameters = parameters ?? Enumerable.Empty<NpgsqlParameter>();

			var projections = new List<T>();

			using (NpgsqlDataReader reader = await ExecuteReader(context, sql, parameters))
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

		protected async Task<IEnumerable<T>> ExecuteProjection<T>(ConnectionContext context, string sql, Func<NpgsqlDataReader, T> getProjectedObjectDelegate, params NpgsqlParameter[] parameters)
		{
			return await ExecuteProjection(context, sql, getProjectedObjectDelegate, (IEnumerable<NpgsqlParameter>)parameters);
		}

		protected IEnumerable<T> ExecuteProjection<T>(ConnectionContext context, string sql, Func<DataRow, T> getProjectedObjectDelegate, IEnumerable<NpgsqlParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");
			getProjectedObjectDelegate.ThrowIfNull("getProjectedObjectDelegate");

			parameters = parameters ?? Enumerable.Empty<NpgsqlParameter>();

			var table = new DataTable();
			string formattedSql = FormatSql(sql);
			NpgsqlCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);
			var dataAdapter = new NpgsqlDataAdapter(command);

			dataAdapter.Fill(table);

			return table.Rows.Cast<DataRow>().Select(getProjectedObjectDelegate);
		}

		protected IEnumerable<T> ExecuteProjection<T>(ConnectionContext context, string sql, Func<DataRow, T> getProjectedObjectDelegate, params NpgsqlParameter[] parameters)
		{
			return ExecuteProjection(context, sql, getProjectedObjectDelegate, (IEnumerable<NpgsqlParameter>)parameters);
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