using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

using Junior.Common;

namespace Junior.Data
{
	public abstract class DataConnector<TConnectionContext, TCommand, TDataReader, TDataAdapter, TParameter, TType>
		where TConnectionContext : class, IConnectionContext
		where TCommand : DbCommand
		where TDataReader : DbDataReader
		where TDataAdapter : DbDataAdapter
		where TParameter : DbParameter
		where TType : struct
	{
		private readonly ICommandProvider<TConnectionContext, TCommand, TParameter> _commandProvider;

		protected DataConnector(ICommandProvider<TConnectionContext, TCommand, TParameter> commandProvider)
		{
			commandProvider.ThrowIfNull("commandProvider");

			_commandProvider = commandProvider;
		}

		protected abstract TParameter GetParameter(string parameterName, object value);
		protected abstract TParameter GetParameter(string parameterName, object value, TType type);
		protected abstract TParameter GetParameter(string parameterName, object value, TType type, int size);
		protected abstract TParameter GetParameter(string parameterName, object value, TType type, int size, byte precision, byte scale);
		protected abstract TParameter GetParameter(string parameterName, object value, TType type, byte precision, byte scale);
		protected abstract TDataAdapter GetDataAdapter(TCommand command);

		protected async Task<int> ExecuteNonQuery(TConnectionContext context, string sql, IEnumerable<TParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<TParameter>();

			string formattedSql = FormatSql(sql);
			TCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);

			return await command.ExecuteNonQueryAsync();
		}

		protected async Task<int> ExecuteNonQuery(TConnectionContext context, string sql, params TParameter[] parameters)
		{
			return await ExecuteNonQuery(context, sql, (IEnumerable<TParameter>)parameters);
		}

		protected async Task<T> ExecuteScalar<T>(TConnectionContext context, string sql, IEnumerable<TParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<TParameter>();

			string formattedSql = FormatSql(sql);
			TCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);
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

		protected async Task<T> ExecuteScalar<T>(TConnectionContext context, string sql, params TParameter[] parameters)
		{
			return await ExecuteScalar<T>(context, sql, (IEnumerable<TParameter>)parameters);
		}

		protected async Task<TDataReader> ExecuteReader(TConnectionContext context, CommandBehavior commandBehavior, string sql, IEnumerable<TParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");

			parameters = parameters ?? Enumerable.Empty<TParameter>();

			string formattedSql = FormatSql(sql);
			TCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);

			return (TDataReader)await command.ExecuteReaderAsync(commandBehavior);
		}

		protected async Task<TDataReader> ExecuteReader(TConnectionContext context, CommandBehavior commandBehavior, string sql, params TParameter[] parameters)
		{
			return await ExecuteReader(context, commandBehavior, sql, (IEnumerable<TParameter>)parameters);
		}

		protected async Task<TDataReader> ExecuteReader(TConnectionContext context, string sql, IEnumerable<TParameter> parameters)
		{
			return await ExecuteReader(context, CommandBehavior.Default, sql, parameters);
		}

		protected async Task<TDataReader> ExecuteReader(TConnectionContext context, string sql, params TParameter[] parameters)
		{
			return await ExecuteReader(context, CommandBehavior.Default, sql, (IEnumerable<TParameter>)parameters);
		}

		protected async Task<IEnumerable<T>> ExecuteProjection<T>(TConnectionContext context, string sql, Func<TDataReader, T> getProjectedObjectDelegate, IEnumerable<TParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");
			getProjectedObjectDelegate.ThrowIfNull("getProjectedObjectDelegate");

			parameters = parameters ?? Enumerable.Empty<TParameter>();

			var projections = new List<T>();

			using (TDataReader reader = await ExecuteReader(context, sql, parameters))
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

		protected async Task<IEnumerable<T>> ExecuteProjection<T>(TConnectionContext context, string sql, Func<TDataReader, T> getProjectedObjectDelegate, params TParameter[] parameters)
		{
			return await ExecuteProjection(context, sql, getProjectedObjectDelegate, (IEnumerable<TParameter>)parameters);
		}

		protected IEnumerable<T> ExecuteProjection<T>(TConnectionContext context, string sql, Func<DataRow, T> getProjectedObjectDelegate, IEnumerable<TParameter> parameters)
		{
			context.ThrowIfNull("context");
			sql.ThrowIfNull("sql");
			getProjectedObjectDelegate.ThrowIfNull("getProjectedObjectDelegate");

			parameters = parameters ?? Enumerable.Empty<TParameter>();

			var table = new DataTable();
			string formattedSql = FormatSql(sql);
			TCommand command = _commandProvider.GetCommand(context, formattedSql, parameters);
			TDataAdapter dataAdapter = GetDataAdapter(command);

			dataAdapter.Fill(table);

			return table.Rows.Cast<DataRow>().Select(getProjectedObjectDelegate);
		}

		protected IEnumerable<T> ExecuteProjection<T>(TConnectionContext context, string sql, Func<DataRow, T> getProjectedObjectDelegate, params TParameter[] parameters)
		{
			return ExecuteProjection(context, sql, getProjectedObjectDelegate, (IEnumerable<TParameter>)parameters);
		}

		protected static object GetParameterValue(object value)
		{
			return value ?? DBNull.Value;
		}

		private static string FormatSql(string sql)
		{
			return sql.Trim();
		}
	}
}