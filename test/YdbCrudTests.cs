using System;
using System.Linq;
using Linq2db.Ydb;
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
        /// Creates a real simple_entity table (NOT temporary).
        /// If the table already exists, tries to drop it.
        /// Returns ITable for further queries.
        /// </summary>
        private static ITable<SimpleEntity> CreateSimpleEntityTable(DataConnection db)
        {
            try
            {
                db.DropTable<SimpleEntity>();
            }
            catch
            {
                // ignore if the table does not exist
            }

            db.CreateTable<SimpleEntity>();

            return db.GetTable<SimpleEntity>();
        }

        // ===================== TESTS =====================

        [Test]
        public void CanCreateTable()
        {
            using var db = CreateYdbConnection();
            var table = CreateSimpleEntityTable(db);

            var count = table.Count();

            Assert.That(count, Is.EqualTo(0), "Table must be empty right after creation.");

            db.DropTable<SimpleEntity>();
        }

        [Test]
        public void CanInsertAndSelect()
        {
            using var db = CreateYdbConnection();
            var table = CreateSimpleEntityTable(db);

            var now = DateTime.UtcNow;

            var entity = new SimpleEntity
            {
                Id      = 1,
                IntVal  = 42,
                DecVal  = 3.14m,
                StrVal  = "hello",
                BoolVal = true,
                DtVal   = now
            };

            Assert.DoesNotThrow(
                () => db.Insert(entity),
                "Insert into YDB must not throw."
            );

            var loaded = table.SingleOrDefault(e => e.Id == 1);

            Assert.That(loaded, Is.Not.Null, "Row with Id = 1 must exist.");

            Assert.Multiple(() =>
            {
                Assert.That(loaded!.IntVal,  Is.EqualTo(42));
                Assert.That(loaded.DecVal,   Is.EqualTo(3.14m));
                Assert.That(loaded.StrVal,   Is.EqualTo("hello"));
                Assert.That(loaded.BoolVal,  Is.True);
                Assert.That(
                    loaded.DtVal,
                    Is.EqualTo(now).Within(TimeSpan.FromSeconds(1)),
                    "DtVal must match the inserted value within 1 second."
                );
            });

            db.DropTable<SimpleEntity>();
        }

        [Test]
        public void CanUpdateByPrimaryKey()
        {
            using var db = CreateYdbConnection();
            var table = CreateSimpleEntityTable(db);

            var now = DateTime.UtcNow;

            var entity = new SimpleEntity
            {
                Id      = 10,
                IntVal  = 1,
                DecVal  = 1.23m,
                StrVal  = "old",
                BoolVal = false,
                DtVal   = now
            };

            db.Insert(entity);

            var updated = new SimpleEntity
            {
                Id      = 10,
                IntVal  = 99,
                DecVal  = 9.99m,
                StrVal  = "updated",
                BoolVal = true,
                DtVal   = now.AddDays(1)
            };

            Assert.DoesNotThrow(
                () => db.Update(updated),
                "Update must not throw."
            );

            var loaded = table.Single(e => e.Id == 10);

            Assert.Multiple(() =>
            {
                Assert.That(loaded.IntVal,  Is.EqualTo(99));
                Assert.That(loaded.DecVal,  Is.EqualTo(9.99m));
                Assert.That(loaded.StrVal,  Is.EqualTo("updated"));
                Assert.That(loaded.BoolVal, Is.True);
                Assert.That(
                    loaded.DtVal,
                    Is.EqualTo(now.AddDays(1)).Within(TimeSpan.FromSeconds(1)),
                    "DtVal must be updated and within 1 second tolerance."
                );
            });

            db.DropTable<SimpleEntity>();
        }

        [Test]
        public void CanDeleteByPrimaryKey()
        {
            using var db = CreateYdbConnection();
            var table = CreateSimpleEntityTable(db);

            var now = DateTime.UtcNow;

            var entity = new SimpleEntity
            {
                Id      = 100,
                IntVal  = 7,
                DecVal  = 0.5m,
                StrVal  = "to_delete",
                BoolVal = false,
                DtVal   = now
            };

            db.Insert(entity);

            var before = table.Count();
            Assert.That(table.Any(e => e.Id == 100), Is.True, "Row must exist before delete.");

            db.Delete(new SimpleEntity { Id = 100 });

            var after = table.Count();

            Assert.Multiple(() =>
            {
                Assert.That(after, Is.EqualTo(before - 1), "Row count must decrease by 1 after delete.");
                Assert.That(table.Any(e => e.Id == 100), Is.False, "Row with Id = 100 must not exist after delete.");
            });

            db.DropTable<SimpleEntity>();
        }

        [Test]
        public void BulkCopy_Insert_Update_Delete_ManyRows()
        {
            using var db = CreateYdbConnection();
            var table = CreateSimpleEntityTable(db);

            const int batchSize = 5_000;

            var now = DateTime.UtcNow;

            var data = Enumerable
                .Range(0, batchSize)
                .Select(i => new SimpleEntity
                {
                    Id      = i,
                    IntVal  = i,
                    DecVal  = 0m,
                    StrVal  = "Name " + i.ToString(),
                    BoolVal = (i % 2) == 0,
                    DtVal   = now
                });

            var copyResult = db.BulkCopy(data);

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

            Assert.Multiple(() =>
            {
                Assert.That(left, Is.EqualTo(0), "Table must be empty after bulk delete.");
            });

            db.DropTable<SimpleEntity>();
        }
    }
}
