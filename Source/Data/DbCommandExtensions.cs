using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

using Junior.Common;

namespace Junior.Data
{
	public static class DbCommandExtensions
	{
		public static async Task OpenConnectionAsync(this DbCommand command)
		{
			command.ThrowIfNull("command");

			if (command.Connection != null && command.Connection.State == ConnectionState.Closed)
			{
				await command.Connection.OpenAsync();
			}
		}
	}
}