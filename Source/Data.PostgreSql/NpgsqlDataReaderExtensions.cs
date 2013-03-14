using System;

using Junior.Common;

using Npgsql;

namespace Junior.Data.PostgreSql
{
	public static class NpgsqlDataReaderExtensions
	{
		public static T GetValue<T>(this NpgsqlDataReader reader, string column)
		{
			reader.ThrowIfNull("reader");
			column.ThrowIfNull("column");

			int ordinal = reader.GetOrdinal(column);
			Type type = typeof(T);

			if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>) && reader.IsDBNull(ordinal))
			{
				return default(T);
			}

			return (T)reader.GetValue(ordinal);
		}
	}
}