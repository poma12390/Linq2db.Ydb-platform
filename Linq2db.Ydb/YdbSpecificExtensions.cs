using System.Linq.Expressions;
using JetBrains.Annotations;
using Linq2db.Ydb.Internal;
using LinqToDB;
using LinqToDB.Internal.Linq;
using LinqToDB.Mapping;
using LinqToDB.SqlQuery;

namespace Linq2db.Ydb
{
	public static class YdbSpecificExtensions
	{
		[LinqTunnel, Pure, IsQueryable]
		[Sql.QueryExtension(null, Sql.QueryExtensionScope.None, typeof(NoneExtensionBuilder))]
		public static IYdbSpecificTable<TSource> AsYdb<TSource>(this ITable<TSource> table)
			where TSource : notnull
		{
			var wrapped = new Table<TSource>(
				table.DataContext,
				Expression.Call(
					null,
					MethodHelper.GetMethodInfo(AsYdb, table),
					table.Expression));

			return new YdbSpecificTable<TSource>(wrapped);
		}

		[LinqTunnel, Pure, IsQueryable]
		[Sql.QueryExtension(null, Sql.QueryExtensionScope.None, typeof(NoneExtensionBuilder))]
		public static IYdbSpecificQueryable<TSource> AsYdb<TSource>(this IQueryable<TSource> source)
			where TSource : notnull
		{
			var normal = source.ProcessIQueryable();

			return new YdbSpecificQueryable<TSource>(
				normal.Provider.CreateQuery<TSource>(
					Expression.Call(
						null,
						MethodHelper.GetMethodInfo(AsYdb, source),
						normal.Expression)));
		}
	}
}
