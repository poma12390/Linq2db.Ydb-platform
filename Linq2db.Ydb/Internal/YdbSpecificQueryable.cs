using LinqToDB.Internal.DataProvider;
using LinqToDB.Internal.DataProvider.Ydb;

namespace Linq2db.Ydb.Internal
{
	sealed class YdbSpecificQueryable<TSource>
		: DatabaseSpecificQueryable<TSource>,
			IYdbSpecificQueryable<TSource>
	{
		public YdbSpecificQueryable(IQueryable<TSource> queryable) : base(queryable) { }
	}
}
