using System.Data.Common;
using System.Threading.Tasks;

namespace Junior.Data
{
	public interface IConnectionResolver<TConnection>
		where TConnection : DbConnection
	{
		Task<IResolvedConnection<TConnection>> ResolveConnectionAsync(string connectionKey, bool openConnection = true);
	}
}