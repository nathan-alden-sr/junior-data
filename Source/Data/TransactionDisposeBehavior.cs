namespace Junior.Data
{
	public enum TransactionDisposeBehavior
	{
		CommitIfNonFinalized,
		RollbackIfNonFinalized,
		None
	}
}