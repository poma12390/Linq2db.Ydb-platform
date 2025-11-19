using Linq2db.Ydb;
using Linq2db.Ydb.Internal;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;
using NUnit.Framework;

namespace Tests.Ydb
{
    [TestFixture]
    public sealed class YdbCrudTests
    {
        // ===================== MODEL =====================

        [Table("simple_entity")]
        public sealed class SimpleEntity
        {
            [Column("id"), PrimaryKey]
            public int Id { get; set; }

            [Column("int_val")]
            public int IntVal { get; set; }

            [Column("dec_val")]
            public decimal DecVal { get; set; }

            [Column("str_val")]
            public string? StrVal { get; set; }

            [Column("bool_val")]
            public bool BoolVal { get; set; }

            [Column("dt_val")]
            public DateTime DtVal { get; set; }
        }

        // ===================== UTILITIES =====================

        private const string DefaultConnectionString =
            "Host=localhost;Port=2136;Database=/local;UseTls=false;DisableDiscovery=true";

        /// <summary>
        /// Creates a DataConnection to YDB using the provider.
        /// The connection string is taken from YDB_CONNECTION_STRING
        /// or the local default is used.
        /// </summary>
        private static DataConnection CreateYdbConnection()
        {
            var fromEnv = Environment.GetEnvironmentVariable("YDB_CONNECTION_STRING");
            var connectionString = string.IsNullOrWhiteSpace(fromEnv)
                ? DefaultConnectionString
                : fromEnv;

            return YdbTools.CreateDataConnection(connectionString);
        }

        /// <summary>
        /// Generates a unique table name for a temporary SimpleEntity table.
        /// </summary>
        private static string GenerateSimpleEntityTableName()
        {
            return $"temp_table_{Guid.NewGuid():N}";
        }

        /// <summary>
        /// Creates SimpleEntity table with the given physical name and returns
        /// ITable bound to that name.
        /// </summary>
        private static ITable<SimpleEntity> CreateSimpleEntityTable(DataConnection db, string tableName)
        {
            db.CreateTable<SimpleEntity>(tableName);
            return db.GetTable<SimpleEntity>().TableName(tableName);
        }

        /// <summary>
        /// Opens connection, creates temporary table with unique name,
        /// runs test body and always drops the table in finally.
        /// </summary>
        private static void RunWithTemporarySimpleEntityTable(
            Action<DataConnection, ITable<SimpleEntity>, string> testBody)
        {
            using var db = CreateYdbConnection();
            var tableName = GenerateSimpleEntityTableName();

            var table = CreateSimpleEntityTable(db, tableName);

            try
            {
                testBody(db, table, tableName);
            }
            finally
            {
                db.DropTable<SimpleEntity>(tableName);
            }
        }

        // ===================== TESTS =====================

        [Test]
        public void CanCreateTable()
        {
            RunWithTemporarySimpleEntityTable((db, table, tableName) =>
            {
                var count = table.Count();

                Assert.That(count, Is.EqualTo(0), "Table must be empty right after creation.");
            });
        }

        [Test]
        public void CanInsertAndSelect()
        {
            RunWithTemporarySimpleEntityTable((db, table, tableName) =>
            {
                var now = DateTime.UtcNow;

                table.Insert(() => new SimpleEntity
                {
                    Id      = 1,
                    IntVal  = 42,
                    DecVal  = 3.14m,
                    StrVal  = "hello",
                    BoolVal = true,
                    DtVal   = now
                });

                var loaded = table.SingleOrDefault(e => e.Id == 1);

                Assert.That(loaded, Is.Not.Null, "Row with Id = 1 must exist.");

                Assert.Multiple(() =>
                {
                    Assert.That(loaded!.IntVal, Is.EqualTo(42));
                    Assert.That(loaded.DecVal, Is.EqualTo(3.14m));
                    Assert.That(loaded.StrVal, Is.EqualTo("hello"));
                    Assert.That(loaded.BoolVal, Is.True);
                    Assert.That(
                        loaded.DtVal,
                        Is.EqualTo(now).Within(TimeSpan.FromSeconds(1)),
                        "DtVal must match the inserted value within 1 second.");
                });
            });
        }

        [Test]
        public void CanUpdateByPrimaryKey()
        {
            RunWithTemporarySimpleEntityTable((db, table, tableName) =>
            {
                var now = DateTime.UtcNow;

                table.Insert(() => new SimpleEntity
                {
                    Id      = 10,
                    IntVal  = 1,
                    DecVal  = 1.23m,
                    StrVal  = "old",
                    BoolVal = false,
                    DtVal   = now
                });

                var newDt = now.AddDays(1);

                table
                    .Where(e => e.Id == 10)
                    .Set(e => e.IntVal,  99)
                    .Set(e => e.DecVal,  9.99m)
                    .Set(e => e.StrVal,  "updated")
                    .Set(e => e.BoolVal, true)
                    .Set(e => e.DtVal,   newDt)
                    .Update();

                var loaded = table.Single(e => e.Id == 10);

                Assert.Multiple(() =>
                {
                    Assert.That(loaded.IntVal, Is.EqualTo(99));
                    Assert.That(loaded.DecVal, Is.EqualTo(9.99m));
                    Assert.That(loaded.StrVal, Is.EqualTo("updated"));
                    Assert.That(loaded.BoolVal, Is.True);
                    Assert.That(
                        loaded.DtVal,
                        Is.EqualTo(newDt).Within(TimeSpan.FromSeconds(1)),
                        "DtVal must be updated and within 1 second tolerance.");
                });
            });
        }

        [Test]
        public void CanDeleteByPrimaryKey()
        {
            RunWithTemporarySimpleEntityTable((db, table, tableName) =>
            {
                var now = DateTime.UtcNow;

                table.Insert(() => new SimpleEntity
                {
                    Id      = 100,
                    IntVal  = 7,
                    DecVal  = 0.5m,
                    StrVal  = "to_delete",
                    BoolVal = false,
                    DtVal   = now
                });

                var before = table.Count();
                Assert.That(table.Any(e => e.Id == 100), Is.True, "Row must exist before delete.");

                table.Delete(e => e.Id == 100);

                var after = table.Count();

                Assert.Multiple(() =>
                {
                    Assert.That(after, Is.EqualTo(before - 1), "Row count must decrease by 1 after delete.");
                    Assert.That(table.Any(e => e.Id == 100), Is.False, "Row with Id = 100 must not exist after delete.");
                });
            });
        }

        [Test]
        public void BulkCopy_Insert_Update_Delete_ManyRows()
        {
            RunWithTemporarySimpleEntityTable((db, table, tableName) =>
            {
                const int batchSize = 5_000;

                var now = DateTime.UtcNow;

                var data = Enumerable
                    .Range(0, batchSize)
                    .Select(i => new SimpleEntity
                    {
                        Id      = i,
                        IntVal  = i,
                        DecVal  = 0m,
                        StrVal  = "Name " + i,
                        BoolVal = (i % 2) == 0,
                        DtVal   = now
                    });
                
                var provider   = (YdbDataProvider)db.DataProvider;
                var bulkHelper = new YdbBulkCopy(provider);

                var copyResult = bulkHelper.BulkCopy(
                    BulkCopyType.ProviderSpecific,
                    table,
                    db.Options,
                    data);

                Assert.That(copyResult.RowsCopied, Is.EqualTo(batchSize), "BulkCopy must insert all rows.");

                var ids = Enumerable.Range(0, batchSize).ToArray();

                var insertedCount = table.Count(t => ids.Contains(t.Id));
                Assert.That(insertedCount, Is.EqualTo(batchSize), "All inserted Id values must be present in the table.");

                const decimal newDec  = 1.23m;
                const string  newStr  = "updated";
                const bool    newBool = true;

                table
                    .Where(t => ids.Contains(t.Id))
                    .Set(t => t.DecVal,  _ => newDec)
                    .Set(t => t.StrVal,  _ => newStr)
                    .Set(t => t.BoolVal, _ => newBool)
                    .Update();

                var mismatchCount = table.Count(t =>
                    ids.Contains(t.Id) &&
                    (t.DecVal != newDec || t.StrVal != newStr || t.BoolVal != newBool));

                Assert.That(mismatchCount, Is.EqualTo(0), "All rows must have updated values after UPDATE.");

                table.Delete(t => ids.Contains(t.Id));
                var left = table.Count();

                Assert.That(left, Is.EqualTo(0), "Table must be empty after bulk delete.");
            });
        }
    }
}
