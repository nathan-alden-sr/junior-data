using System;

using Junior.Common;

using MySql.Data.MySqlClient;

namespace Junior.Data.MySql
{
	public static class MySqlDataReaderExtensions
	{
		public static T GetValue<T>(this MySqlDataReader reader, string column)
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