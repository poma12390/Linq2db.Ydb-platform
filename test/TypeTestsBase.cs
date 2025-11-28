using Linq2db.Ydb;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Interceptors;
using LinqToDB.Mapping;
using NUnit.Framework;
using Ydb.Sdk.Ado;

namespace test
{
    /// <summary>
    /// Base class for YDB-specific type tests.
    /// <para>
    /// Responsibilities:
    /// <list type="bullet">
    /// <item><description>
    /// Creates a simple table with two columns:
    /// a non-nullable value column and a nullable column of the same logical type.
    /// </description></item>
    /// <item><description>
    /// Applies mapping for the type under test using <see cref="DbDataType"/>
    /// (DataType, DbType, precision, scale, length).
    /// </description></item>
    /// <item><description>
    /// Executes several write paths:
    /// inline insert, parameterized select, BulkCopy RowByRow, BulkCopy MultipleRows.
    /// </description></item>
    /// <item><description>
    /// Reads the data back and compares it with the expected values
    /// (optionally using custom expectations).
    /// </description></item>
    /// </list>
    /// </para>
    /// <para>
    /// YDB specifics taken into account:
    /// <list type="bullet">
    /// <item><description>
    /// YDB does not support temporary tables, so each test creates and drops
    /// a real table with a generated name based on the generic type arguments.
    /// </description></item>
    /// <item><description>
    /// <see cref="YdbParameter"/> cannot handle <c>null</c> values without
    /// an explicit type, therefore parameter tests avoid creating parameters
    /// with null values when it is unsafe.
    /// </description></item>
    /// <item><description>
    /// Provider-specific BulkCopy (BulkUpsert) is intentionally not used here,
    /// as it has different type constraints and can introduce unrelated failures.
    /// </description></item>
    /// </list>
    /// </para>
    /// </summary>
    public abstract class TypeTestsBase
    {
        // ===================== TABLE MODEL =====================

        /// <summary>
        /// Simple table model used for all type tests.
        /// <para>
        /// It contains:
        /// <list type="bullet">
        /// <item><description><see cref="Id"/> — primary key, used to identify the row.</description></item>
        /// <item><description><see cref="Column"/> — non-nullable value under test.</description></item>
        /// <item><description><see cref="ColumnNullable"/> — nullable value of the same logical type.</description></item>
        /// </list>
        /// </para>
        /// <para>
        /// The mapping (data type, DbType, precision, scale, length) is configured
        /// dynamically in <see cref="TestType{TType,TNullableType}"/> based on
        /// the supplied <see cref="DbDataType"/>.
        /// </para>
        /// </summary>
        private sealed class TypeTable<TType, TNullableType>
        {
            [PrimaryKey]
            public int Id { get; set; }

            public TType          Column         { get; set; } = default!;
            public TNullableType? ColumnNullable { get; set; }
        }

        // ===================== YDB CONNECTION =====================

        /// <summary>
        /// Tries to parse an integer environment variable; returns <paramref name="fallback"/>
        /// if parsing fails.
        /// </summary>
        private static int TryInt(string? s, int fallback) =>
            int.TryParse(s, out var v) ? v : fallback;

        /// <summary>
        /// Tries to parse a boolean environment variable; returns <paramref name="fallback"/>
        /// if parsing fails.
        /// </summary>
        private static bool TryBool(string? s, bool fallback) =>
            bool.TryParse(s, out var v) ? v : fallback;

        private static readonly string Host    = Environment.GetEnvironmentVariable("YDB_HOST")      ?? "localhost";
        private static readonly int    Port    = TryInt(Environment.GetEnvironmentVariable("YDB_PORT"), 2136);
        private static readonly string DbPath  = Environment.GetEnvironmentVariable("YDB_DB")        ?? "/local";
        private static readonly bool   UseTls  = TryBool(Environment.GetEnvironmentVariable("YDB_USE_TLS"), false);
        private static readonly int    TlsPort = TryInt(Environment.GetEnvironmentVariable("YDB_TLS_PORT"), 2135);

        /// <summary>
        /// Creates a <see cref="DataConnection"/> for YDB using the ADO connection.
        /// <para>
        /// Connection settings are taken from environment variables:
        /// <c>YDB_HOST</c>, <c>YDB_PORT</c>, <c>YDB_DB</c>, <c>YDB_USE_TLS</c>, <c>YDB_TLS_PORT</c>.
        /// Default values point to a local YDB instance.
        /// </para>
        /// </summary>
        private static async Task<DataConnection> CreateYdbConnection()
        {
            var builder = new YdbConnectionStringBuilder
            {
                Host     = Host,
                Port     = UseTls ? TlsPort : Port,
                Database = DbPath,
                UseTls   = UseTls
            };

            await using var ydbConnection = new YdbConnection(builder);
            return YdbTools.CreateDataConnection(ydbConnection);
        }

        /// <summary>
        /// Controls whether parameterized queries should be tested by default.
        /// <para>
        /// Individual tests can override this behavior via the
        /// <c>testParameters</c> argument of <see cref="TestType{TType,TNullableType}"/>.
        /// </para>
        /// </summary>
        protected virtual bool TestParameters => true;

        // ===================== CORE TYPE TEST METHOD =====================

        /// <summary>
        /// Core helper that executes a full round-trip scenario for a given type mapping.
        /// <para>
        /// The method performs the following steps:
        /// <list type="number">
        /// <item><description>
        /// Build a type mapping for <see cref="TypeTable{TType,TNullableType}"/> using
        /// the provided <paramref name="dbType"/> (DataType, DbType, precision, scale, length).
        /// </description></item>
        /// <item><description>
        /// Create a YDB table based on this mapping and insert a single row.
        /// </description></item>
        /// <item><description>
        /// Run parameterized queries (if enabled) and assert the read values.
        /// </description></item>
        /// <item><description>
        /// Run queries with inlined literals (<c>InlineParameters = true</c>) and assert again.
        /// </description></item>
        /// <item><description>
        /// Execute BulkCopy in <see cref="BulkCopyType.RowByRow"/> and
        /// <see cref="BulkCopyType.MultipleRows"/> modes (when applicable),
        /// and verify that the stored values match expectations.
        /// </description></item>
        /// </list>
        /// </para>
        /// <para>
        /// Custom expectations can be provided via:
        /// <list type="bullet">
        /// <item><description><paramref name="getExpectedValue"/> /
        /// <paramref name="getExpectedNullableValue"/> — transform written values
        /// into expected stored values (e.g., truncation or rounding).</description></item>
        /// <item><description><paramref name="isExpectedValue"/> /
        /// <paramref name="isExpectedNullableValue"/> — predicates to validate
        /// the stored values when exact equality is not appropriate.</description></item>
        /// </list>
        /// </para>
        /// </summary>
        protected async ValueTask TestType<TType, TNullableType>(
            DbDataType dbType,
            TType value,
            TNullableType? nullableValue,
            bool? testParameters = null,
            bool skipNullable = false,
            bool filterByValue = true,
            bool filterByNullableValue = true,
            Func<TType, TType>? getExpectedValue = null,
            Func<TNullableType?, TNullableType?>? getExpectedNullableValue = null,
            Func<TType, bool>? isExpectedValue = null,
            Func<TNullableType?, bool>? isExpectedNullableValue = null,
            Func<BulkCopyType, bool>? testBulkCopyType = null)
        {
            testParameters ??= TestParameters;

            // ---------- MappingSchema and FluentMapping ----------

            var ms  = new MappingSchema();
            var ent = new FluentMappingBuilder(ms).Entity<TypeTable<TType, TNullableType>>();

            ent.Property(e => e.Id).IsPrimaryKey();

            var prop = ent.Property(e => e.Column).IsNullable(false);

            // Apply DbDataType configuration to the non-nullable column.
            if (dbType.DataType  != DataType.Undefined) prop.HasDataType(dbType.DataType);
            if (dbType.DbType    != null)              prop.HasDbType(dbType.DbType);
            if (dbType.Precision != null)              prop.HasPrecision(dbType.Precision.Value);
            if (dbType.Scale     != null)              prop.HasScale(dbType.Scale.Value);
            if (dbType.Length    != null)              prop.HasLength(dbType.Length.Value);

            if (!skipNullable)
            {
                // Configure the nullable column in the same way as the main column,
                // so both columns share identical storage settings.
                var propN = ent.Property(e => e.ColumnNullable).IsNullable(true);

                if (dbType.DataType  != DataType.Undefined) propN.HasDataType(dbType.DataType);
                if (dbType.DbType    != null)              propN.HasDbType(dbType.DbType);
                if (dbType.Precision != null)              propN.HasPrecision(dbType.Precision.Value);
                if (dbType.Scale     != null)              propN.HasScale(dbType.Scale.Value);
                if (dbType.Length    != null)              propN.HasLength(dbType.Length.Value);
            }
            else
            {
                // Nullable column is not mapped to the table in this scenario.
                ent.Property(e => e.ColumnNullable).IsNotColumn();
            }

            ent.Build();

            var row = new TypeTable<TType, TNullableType>
            {
                Id             = 1,
                Column         = value,
                ColumnNullable = nullableValue
            };

            DataConnection? db = null;

            try
            {
                // ---------- Connection and table creation ----------
                db = await CreateYdbConnection();
                db.AddMappingSchema(ms);

                // Table name will be based on TypeTable`2 generic name,
                // which is stable and suitable for YDB.
                db.CreateTable<TypeTable<TType, TNullableType>>();

                var table = db.GetTable<TypeTable<TType, TNullableType>>();

                // ---------- Initial single-row insert WITHOUT parameters ----------
                // We start with an inline insert to bypass parameter-related limitations
                // such as null parameter type inference in YdbParameter.
                db.InlineParameters = true;
                table.Insert(() => new TypeTable<TType, TNullableType>
                {
                    Id             = row.Id,
                    Column         = row.Column,
                    ColumnNullable = row.ColumnNullable
                });
                db.InlineParameters = false;

                // ---------- Parameterized query tests ----------
                if (testParameters == true)
                {
                    // Filter by nullable column only if it is not null
                    // (otherwise a parameter with a null value would be created,
                    // which is not supported without an explicit type).
                    var filterByNullableForParams =
                        filterByNullableValue && !skipNullable && nullableValue is not null;

                    db.InlineParameters = false;

                    var expectedParamCount =
                        (filterByValue ? 1 : 0) +
                        (filterByNullableForParams ? 1 : 0);

                    db.OnNextCommandInitialized((_, cmd) =>
                    {
                        Assert.That(cmd.Parameters, Has.Count.EqualTo(expectedParamCount));
                        return cmd;
                    });

                    AssertData(
                        table,
                        value,
                        nullableValue,
                        skipNullable,
                        filterByValue,
                        filterByNullableForParams,
                        getExpectedValue,
                        getExpectedNullableValue,
                        isExpectedValue,
                        isExpectedNullableValue);
                }

                // ---------- Literal tests (InlineParameters = true) ----------
                // Here we force all values to be inlined into SQL text and
                // assert that the provider does not generate parameters.
                {
                    db.InlineParameters = true;

                    db.OnNextCommandInitialized((_, cmd) =>
                    {
                        Assert.That(cmd.Parameters, Is.Empty);
                        return cmd;
                    });

                    AssertData(
                        table,
                        value,
                        nullableValue,
                        skipNullable,
                        filterByValue,
                        filterByNullableValue,
                        getExpectedValue,
                        getExpectedNullableValue,
                        isExpectedValue,
                        isExpectedNullableValue);

                    db.InlineParameters = false;
                }

                // If parameter tests are disabled, always inline parameters
                // for the remaining operations (BulkCopy).
                if (testParameters == false)
                    db.InlineParameters = true;

                // For cases with nullableValue == null we skip BulkCopy
                // to avoid creating a parameter with a null value that YDB
                // cannot infer type for.
                var canBulkCopy = nullableValue is not null;

                // ---------- BulkCopy: RowByRow ----------
                if (canBulkCopy && (testBulkCopyType == null || testBulkCopyType(BulkCopyType.RowByRow)))
                {
                    var options = new BulkCopyOptions
                    {
                        BulkCopyType  = BulkCopyType.RowByRow,
                        UseParameters = testParameters != false
                    };

                    table.Delete();
                    db.BulkCopy(options, new[] { row });

                    AssertData(
                        table,
                        value,
                        nullableValue,
                        skipNullable,
                        filterByValue,
                        filterByNullableValue,
                        getExpectedValue,
                        getExpectedNullableValue,
                        isExpectedValue,
                        isExpectedNullableValue);
                }

                // ---------- BulkCopy: MultipleRows ----------
                if (canBulkCopy && (testBulkCopyType == null || testBulkCopyType(BulkCopyType.MultipleRows)))
                {
                    var options = new BulkCopyOptions
                    {
                        BulkCopyType  = BulkCopyType.MultipleRows,
                        UseParameters = testParameters != false
                    };

                    table.Delete();
                    db.BulkCopy(options, new[] { row });

                    AssertData(
                        table,
                        value,
                        nullableValue,
                        skipNullable,
                        filterByValue,
                        filterByNullableValue,
                        getExpectedValue,
                        getExpectedNullableValue,
                        isExpectedValue,
                        isExpectedNullableValue);
                }

                // NOTE:
                // ProviderSpecific BulkCopy (BulkUpsert) is intentionally NOT used here
                // to avoid additional provider-specific complexity. These tests focus on
                // basic insert/select and standard BulkCopy behaviors.
            }
            finally
            {
                if (db != null)
                {
                    try
                    {
                        db.DropTable<TypeTable<TType, TNullableType>>();
                    }
                    catch
                    {
                        // The table may already be dropped or unavailable; ignore.
                    }

                    db.Dispose();
                }
            }
        }

        // ===================== DATA ASSERTION =====================

        /// <summary>
        /// Reads rows from the test table and verifies that the stored values
        /// match the expected ones.
        /// <para>
        /// Filtering behavior:
        /// <list type="bullet">
        /// <item><description>
        /// If <paramref name="filterByValue"/> is <c>true</c>, records are filtered by
        /// the non-nullable column (<see cref="TypeTable{TType,TNullableType}.Column"/>).
        /// </description></item>
        /// <item><description>
        /// If <paramref name="filterByNullableValue"/> is <c>true</c> and nullable
        /// checks are not skipped, records are filtered by
        /// <see cref="TypeTable{TType,TNullableType}.ColumnNullable"/>.
        /// </description></item>
        /// <item><description>
        /// If both flags are <c>false</c>, all rows are read and only the first is used.
        /// </description></item>
        /// </list>
        /// </para>
        /// <para>
        /// Expected values can be provided either as transformation functions
        /// (<paramref name="getExpectedValue"/>, <paramref name="getExpectedNullableValue"/>)
        /// or as boolean predicates (<paramref name="isExpectedValue"/>,
        /// <paramref name="isExpectedNullableValue"/>). If neither is provided, a strict
        /// equality check is used.
        /// </para>
        /// </summary>
        private static void AssertData<TType, TNullableType>(
            ITable<TypeTable<TType, TNullableType>> table,
            TType value,
            TNullableType? nullableValue,
            bool skipNullable,
            bool filterByValue,
            bool filterByNullableValue,
            Func<TType, TType>? getExpectedValue,
            Func<TNullableType?, TNullableType?>? getExpectedNullableValue,
            Func<TType, bool>? isExpectedValue,
            Func<TNullableType?, bool>? isExpectedNullableValue)
        {
            filterByNullableValue = filterByNullableValue && !skipNullable;

            var records =
                filterByValue && filterByNullableValue
                    ? table.Where(r => (object)r.Column! == (object)value! && (object?)r.ColumnNullable == (object?)nullableValue).ToArray()
                    : filterByValue
                        ? table.Where(r => (object)r.Column! == (object)value!).ToArray()
                        : filterByNullableValue
                            ? table.Where(r => (object?)r.ColumnNullable == (object?)nullableValue).ToArray()
                            : table.ToArray();

            Assert.That(records, Has.Length.EqualTo(1));

            var record = records[0];

            // ----- Non-nullable column assertion -----
            if (isExpectedValue != null)
            {
                Assert.That(isExpectedValue(record.Column), Is.True);
            }
            else
            {
                Assert.That(
                    record.Column,
                    Is.EqualTo(
                        getExpectedValue != null
                            ? getExpectedValue(value)
                            : value));
            }

            // ----- Nullable column assertion (if it is mapped) -----
            if (!skipNullable)
            {
                if (isExpectedNullableValue != null)
                {
                    Assert.That(isExpectedNullableValue(record.ColumnNullable), Is.True);
                }
                else
                {
                    Assert.That(
                        record.ColumnNullable,
                        Is.EqualTo(
                            getExpectedNullableValue != null
                                ? getExpectedNullableValue(nullableValue)
                                : nullableValue));
                }
            }
        }
    }
}
