using LinqToDB;
using LinqToDB.Internal.DataProvider;
using LinqToDB.Internal.DataProvider.Ydb;

namespace Linq2db.Ydb.Internal
{
	sealed class YdbSpecificTable<TSource>
		: DatabaseSpecificTable<TSource>,
			IYdbSpecificTable<TSource>
		where TSource : notnull
	{
		public YdbSpecificTable(ITable<TSource> table) : base(table) { }
	}
}
