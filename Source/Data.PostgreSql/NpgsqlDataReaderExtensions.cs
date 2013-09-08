using Junior.Common;

using Npgsql;

namespace Junior.Data.PostgreSql
{
	public static class NpgsqlDataReaderExtensions
	{
		public static T GetValue<T>(this NpgsqlDataReader reader, string column, T defaultValue = default(T))
		{
			reader.ThrowIfNull("reader");
			column.ThrowIfNull("column");

			int ordinal = reader.GetOrdinal(column);

			return GetValue(reader, ordinal, defaultValue);
		}

		public static T GetValue<T>(this NpgsqlDataReader reader, int ordinal, T defaultValue = default(T))
		{
			reader.ThrowIfNull("reader");

			return reader.IsDBNull(ordinal) ? defaultValue : (T)reader.GetValue(ordinal);
		}
	}
}