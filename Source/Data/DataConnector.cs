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
		private readonly IConnectionResolver<TConnection> _connectionResolver;

		protected DataConnector(IConnectionResolver<TConnection> connectionResolver, ICommandTimeoutProvider commandTimeoutProvider, string connectionKey)
		{
			connectionResolver.ThrowIfNull("connectionResolver");
			commandTimeoutProvider.ThrowIfNull("commandTimeoutProvider");

			_connectionResolver = connectionResolver;
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

			using (IResolvedConnection<TConnection> resolvedConnection = await _connectionResolver.ResolveConnectionAsync(_connectionKey))
			{
				TCommand command = CreateCommand(resolvedConnection.Connection, formattedSql, parameters);

				return await command.ExecuteNonQueryAsync();
			}
		}

		protected Task<int> ExecuteNonQueryAsync(string sql, params TParameter[] parameters)
		{
			return ExecuteNonQueryAsync(sql, (IEnumerable<TParameter>)parameters);
		}

		protected async Task<T> ExecuteScalarAsync<T>(string sql, IEnumerable<TParameter> parameters)
		{
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<TParameter>();

			string formattedSql = FormatSql(sql);

			using (IResolvedConnection<TConnection> resolvedConnection = await _connectionResolver.ResolveConnectionAsync(_connectionKey))
			{
				TCommand command = CreateCommand(resolvedConnection.Connection, formattedSql, parameters);

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

		protected Task<T> ExecuteScalarAsync<T>(string sql, params TParameter[] parameters)
		{
			return ExecuteScalarAsync<T>(sql, (IEnumerable<TParameter>)parameters);
		}

		protected async Task<TDataReader> ExecuteReaderAsync(CommandBehavior commandBehavior, string sql, IEnumerable<TParameter> parameters)
		{
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<TParameter>();

			string formattedSql = FormatSql(sql);
			IResolvedConnection<TConnection> resolvedConnection = await _connectionResolver.ResolveConnectionAsync(_connectionKey);

			try
			{
				TCommand command = CreateCommand(resolvedConnection.Connection, formattedSql, parameters);

				return (TDataReader)await command.ExecuteReaderAsync(commandBehavior);
			}
			catch
			{
				resolvedConnection.Dispose();
				throw;
			}
		}

		protected Task<TDataReader> ExecuteReaderAsync(CommandBehavior commandBehavior, string sql, params TParameter[] parameters)
		{
			return ExecuteReaderAsync(commandBehavior, sql, (IEnumerable<TParameter>)parameters);
		}

		protected Task<TDataReader> ExecuteReaderAsync(string sql, IEnumerable<TParameter> parameters)
		{
			return ExecuteReaderAsync(CommandBehavior.CloseConnection, sql, parameters);
		}

		protected Task<TDataReader> ExecuteReaderAsync(string sql, params TParameter[] parameters)
		{
			return ExecuteReaderAsync(CommandBehavior.CloseConnection, sql, (IEnumerable<TParameter>)parameters);
		}

		protected async Task<IEnumerable<T>> ExecuteProjectionAsync<T>(CommandBehavior commandBehavior, string sql, Func<TDataReader, T> getProjectedObjectDelegate, IEnumerable<TParameter> parameters)
		{
			sql.ThrowIfNull("sql");
			getProjectedObjectDelegate.ThrowIfNull("getProjectedObjectDelegate");

			parameters = parameters ?? Enumerable.Empty<TParameter>();

			var projections = new List<T>();

			using (TDataReader reader = await ExecuteReaderAsync(commandBehavior, sql, parameters))
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

		protected Task<IEnumerable<T>> ExecuteProjectionAsync<T>(CommandBehavior commandBehavior, string sql, Func<TDataReader, T> getProjectedObjectDelegate, params TParameter[] parameters)
		{
			return ExecuteProjectionAsync(commandBehavior, sql, getProjectedObjectDelegate, (IEnumerable<TParameter>)parameters);
		}

		protected Task<IEnumerable<T>> ExecuteProjectionAsync<T>(string sql, Func<TDataReader, T> getProjectedObjectDelegate, IEnumerable<TParameter> parameters)
		{
			return ExecuteProjectionAsync(CommandBehavior.CloseConnection, sql, getProjectedObjectDelegate, parameters);
		}

		protected Task<IEnumerable<T>> ExecuteProjectionAsync<T>(string sql, Func<TDataReader, T> getProjectedObjectDelegate, params TParameter[] parameters)
		{
			return ExecuteProjectionAsync(CommandBehavior.CloseConnection, sql, getProjectedObjectDelegate, (IEnumerable<TParameter>)parameters);
		}

		protected async Task<IEnumerable<T>> ExecuteProjectionAsync<T>(string sql, Func<DataRow, T> getProjectedObjectDelegate, IEnumerable<TParameter> parameters)
		{
			sql.ThrowIfNull("sql");
			getProjectedObjectDelegate.ThrowIfNull("getProjectedObjectDelegate");

			parameters = parameters ?? Enumerable.Empty<TParameter>();

			var table = new DataTable();
			string formattedSql = FormatSql(sql);

			using (IResolvedConnection<TConnection> resolvedConnection = await _connectionResolver.ResolveConnectionAsync(_connectionKey))
			{
				TCommand command = CreateCommand(resolvedConnection.Connection, formattedSql, parameters);
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