using System.Data.Common;
using System.Threading.Tasks;

namespace Junior.Data
{
	public interface IConnectionProvider<TConnection>
		where TConnection : DbConnection
	{
		Task<TConnection> GetConnection(string connectionKey, bool openConnection);
	}
}