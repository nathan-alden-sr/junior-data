using System.Data;
using System.Threading.Tasks;

namespace Junior.Data
{
	public interface IConnectionContextFactory<T>
		where T : class, IConnectionContext
	{
		Task<T> CreateAsync(string connectionKey);
		Task<T> CreateWithTransactionAsync(string connectionKey, TransactionDisposeBehavior transactionDisposeBehavior = TransactionDisposeBehavior.RollbackIfNonFinalized);

		Task<T> CreateWithTransactionAsync(
			string connectionKey,
			IsolationLevel isolationLevel,
			TransactionDisposeBehavior transactionDisposeBehavior = TransactionDisposeBehavior.RollbackIfNonFinalized);
	}
}