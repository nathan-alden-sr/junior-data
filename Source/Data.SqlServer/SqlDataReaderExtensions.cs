using System;
using System.Data.SqlClient;

using Junior.Common;

namespace Junior.Data.SqlServer
{
	public static class SqlDataReaderExtensions
	{
		public static T GetValue<T>(this SqlDataReader reader, string column, T defaultValue = default(T))
		{
			reader.ThrowIfNull("reader");
			column.ThrowIfNull("column");

			int ordinal = reader.GetOrdinal(column);

			return GetValue(reader, ordinal, defaultValue);
		}

		public static T GetValue<T>(this SqlDataReader reader, int ordinal, T defaultValue = default(T))
		{
			reader.ThrowIfNull("reader");

			Type type = typeof(T);

			if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>) && reader.IsDBNull(ordinal))
			{
				return defaultValue;
			}

			return (T)reader.GetValue(ordinal);
		}
	}
}