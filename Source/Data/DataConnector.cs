using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

using Junior.Common;

namespace Junior.Data
{
	public abstract class DataConnector<TConnection, TCommand, TDataReader, TDataAdapter, TParameter, TType>
		where TConnection : DbConnection
		where TCommand : DbCommand
		where TDataReader : DbDataReader
		where TDataAdapter : DbDataAdapter
		where TParameter : DbParameter
		where TType : struct
	{
		private readonly ICommandTimeoutProvider _commandTimeoutProvider;
		private readonly string _connectionKey;
		private readonly IConnectionProvider<TConnection> _connectionProvider;

		protected DataConnector(IConnectionProvider<TConnection> connectionProvider, ICommandTimeoutProvider commandTimeoutProvider, string connectionKey)
		{
			connectionProvider.ThrowIfNull("connectionProvider");
			commandTimeoutProvider.ThrowIfNull("commandTimeoutProvider");

			_connectionProvider = connectionProvider;
			_commandTimeoutProvider = commandTimeoutProvider;
			_connectionKey = connectionKey;
		}

		protected abstract TParameter GetParameter(string parameterName, object value);
		protected abstract TParameter GetParameter(string parameterName, object value, TType type);
		protected abstract TParameter GetParameter(string parameterName, object value, TType type, int size);
		protected abstract TParameter GetParameter(string parameterName, object value, TType type, int size, byte precision, byte scale);
		protected abstract TParameter GetParameter(string parameterName, object value, TType type, byte precision, byte scale);
		protected abstract TDataAdapter GetDataAdapter(TCommand command);

		protected async Task<int> ExecuteNonQueryAsync(string sql, IEnumerable<TParameter> parameters)
		{
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<TParameter>();

			string formattedSql = FormatSql(sql);

			using (TConnection connection = await _connectionProvider.GetConnectionAsync(_connectionKey, true))
			{
				TCommand command = CreateCommand(connection, formattedSql, parameters);

				return await command.ExecuteNonQueryAsync();
			}
		}

		protected async Task<int> ExecuteNonQueryAsync(string sql, params TParameter[] parameters)
		{
			return await ExecuteNonQueryAsync(sql, (IEnumerable<TParameter>)parameters);
		}

		protected async Task<T> ExecuteScalarAsync<T>(string sql, IEnumerable<TParameter> parameters)
		{
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<TParameter>();

			string formattedSql = FormatSql(sql);

			using (TConnection connection = await _connectionProvider.GetConnectionAsync(_connectionKey, true))
			{
				TCommand command = CreateCommand(connection, formattedSql, parameters);

				object value = await command.ExecuteScalarAsync();

				if (value != null && value != DBNull.Value)
				{
					return (T)value;
				}

				Type type = typeof(T);

				if (type.IsValueType && (!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(Nullable<>)))
				{
					throw new Exception("Query resulted in NULL but scalar type is a non-nullable value type.");
				}

				return default(T);
			}
		}

		protected async Task<T> ExecuteScalarAsync<T>(string sql, params TParameter[] parameters)
		{
			return await ExecuteScalarAsync<T>(sql, (IEnumerable<TParameter>)parameters);
		}

		protected async Task<TDataReader> ExecuteReaderAsync(CommandBehavior commandBehavior, string sql, IEnumerable<TParameter> parameters)
		{
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<TParameter>();

			string formattedSql = FormatSql(sql);
			TConnection connection = await _connectionProvider.GetConnectionAsync(_connectionKey, true);

			try
			{
				TCommand command = CreateCommand(connection, formattedSql, parameters);

				return (TDataReader)await command.ExecuteReaderAsync(commandBehavior);
			}
			catch
			{
				connection.Dispose();
				throw;
			}
		}

		protected async Task<TDataReader> ExecuteReaderAsync(CommandBehavior commandBehavior, string sql, params TParameter[] parameters)
		{
			return await ExecuteReaderAsync(commandBehavior, sql, (IEnumerable<TParameter>)parameters);
		}

		protected async Task<TDataReader> ExecuteReaderAsync(string sql, IEnumerable<TParameter> parameters)
		{
			return await ExecuteReaderAsync(CommandBehavior.Default, sql, parameters);
		}

		protected async Task<TDataReader> ExecuteReaderAsync(string sql, params TParameter[] parameters)
		{
			return await ExecuteReaderAsync(CommandBehavior.Default, sql, (IEnumerable<TParameter>)parameters);
		}

		protected async Task<IEnumerable<T>> ExecuteProjectionAsync<T>(string sql, Func<TDataReader, T> getProjectedObjectDelegate, IEnumerable<TParameter> parameters)
		{
			sql.ThrowIfNull("sql");
			getProjectedObjectDelegate.ThrowIfNull("getProjectedObjectDelegate");

			parameters = parameters ?? Enumerable.Empty<TParameter>();

			var projections = new List<T>();

			using (TDataReader reader = await ExecuteReaderAsync(sql, parameters))
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

		protected async Task<IEnumerable<T>> ExecuteProjectionAsync<T>(string sql, Func<TDataReader, T> getProjectedObjectDelegate, params TParameter[] parameters)
		{
			return await ExecuteProjectionAsync(sql, getProjectedObjectDelegate, (IEnumerable<TParameter>)parameters);
		}

		protected async Task<IEnumerable<T>> ExecuteProjectionAsync<T>(string sql, Func<DataRow, T> getProjectedObjectDelegate, IEnumerable<TParameter> parameters)
		{
			sql.ThrowIfNull("sql");
			getProjectedObjectDelegate.ThrowIfNull("getProjectedObjectDelegate");

			parameters = parameters ?? Enumerable.Empty<TParameter>();

			var table = new DataTable();
			string formattedSql = FormatSql(sql);

			using (TConnection connection = await _connectionProvider.GetConnectionAsync(_connectionKey, true))
			{
				TCommand command = CreateCommand(connection, formattedSql, parameters);
				TDataAdapter dataAdapter = GetDataAdapter(command);

				dataAdapter.Fill(table);
			}

			return table.Rows.Cast<DataRow>().Select(getProjectedObjectDelegate);
		}

		protected Task<IEnumerable<T>> ExecuteProjectionAsync<T>(string sql, Func<DataRow, T> getProjectedObjectDelegate, params TParameter[] parameters)
		{
			return ExecuteProjectionAsync(sql, getProjectedObjectDelegate, (IEnumerable<TParameter>)parameters);
		}

		protected static object GetParameterValue(object value)
		{
			return value ?? DBNull.Value;
		}

		private TCommand CreateCommand(TConnection connection, string sql, IEnumerable<TParameter> parameters)
		{
			parameters = parameters ?? Enumerable.Empty<TParameter>();

			var command = (TCommand)connection.CreateCommand();

			command.CommandText = sql;
			command.CommandTimeout = (int)_commandTimeoutProvider.GetTimeout(_connectionKey).TotalSeconds;
			command.CommandType = CommandType.Text;

			foreach (TParameter parameter in parameters)
			{
				command.Parameters.Add(parameter);
			}

			return command;
		}

		private static string FormatSql(string sql)
		{
			return sql.Trim();
		}
	}
}