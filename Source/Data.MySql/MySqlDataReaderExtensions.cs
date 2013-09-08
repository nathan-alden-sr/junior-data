using Junior.Common;

using MySql.Data.MySqlClient;

namespace Junior.Data.MySql
{
	public static class MySqlDataReaderExtensions
	{
		public static T GetValue<T>(this MySqlDataReader reader, string column, T defaultValue = default(T))
		{
			reader.ThrowIfNull("reader");
			column.ThrowIfNull("column");

			int ordinal = reader.GetOrdinal(column);

			return GetValue(reader, ordinal, defaultValue);
		}

		public static T GetValue<T>(this MySqlDataReader reader, int ordinal, T defaultValue = default(T))
		{
			reader.ThrowIfNull("reader");

			return reader.IsDBNull(ordinal) ? defaultValue : (T)reader.GetValue(ordinal);
		}
	}
}