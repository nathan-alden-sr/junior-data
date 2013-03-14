namespace Junior.Data
{
	public interface IConnectionStringProvider
	{
		string ByKey(string key);
	}
}