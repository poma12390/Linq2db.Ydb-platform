using LinqToDB;

namespace Linq2db.Ydb
{
	public interface IYdbSpecificTable<out TSource> : ITable<TSource>
		where TSource : notnull
	{
	}
}
