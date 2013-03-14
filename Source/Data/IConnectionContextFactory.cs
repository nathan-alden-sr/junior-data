using System.Data;
using System.Threading.Tasks;

namespace Junior.Data
{
	public interface IConnectionContextFactory<T>
		where T : class, IConnectionContext
	{
		Task<T> Create(string connectionKey);
		Task<T> CreateWithTransaction(string connectionKey, TransactionDisposeBehavior transactionDisposeBehavior = TransactionDisposeBehavior.RollbackIfNonFinalized);

		Task<T> CreateWithTransaction(
			string connectionKey,
			IsolationLevel isolationLevel,
			TransactionDisposeBehavior transactionDisposeBehavior = TransactionDisposeBehavior.RollbackIfNonFinalized);
	}
}