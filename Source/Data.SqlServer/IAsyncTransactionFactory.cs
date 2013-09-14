using System;
using System.Collections.Generic;
using System.Transactions;

namespace Junior.Data.SqlServer
{
	public interface IAsyncTransactionFactory
	{
		IAsyncTransaction Create(TransactionScopeOption transactionScopeOption, TransactionOptions transactionOptions, IEnumerable<string> connectionKeys);
		IAsyncTransaction Create(TransactionScopeOption transactionScopeOption, TransactionOptions transactionOptions, params string[] connectionKeys);
		IAsyncTransaction Create(IsolationLevel isolationLevel, TimeSpan timeout, IEnumerable<string> connectionKeys);
		IAsyncTransaction Create(IsolationLevel isolationLevel, TimeSpan timeout, params string[] connectionKeys);
		IAsyncTransaction Create(IsolationLevel isolationLevel, IEnumerable<string> connectionKeys);
		IAsyncTransaction Create(IsolationLevel isolationLevel, params string[] connectionKeys);
		IAsyncTransaction Create(TimeSpan timeout, IEnumerable<string> connectionKeys);
		IAsyncTransaction Create(TimeSpan timeout, params string[] connectionKeys);
		IAsyncTransaction Create(IEnumerable<string> connectionKeys);
		IAsyncTransaction Create(params string[] connectionKeys);
	}
}