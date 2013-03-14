using System;
using System.Data.SqlClient;

using Junior.Common;

namespace Junior.Data.SqlServer
{
	public static class SqlDataReaderExtensions
	{
		public static T GetValue<T>(this SqlDataReader reader, string column)
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