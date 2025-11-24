using System.Globalization;
using LinqToDB;
using LinqToDB.Data;
using NUnit.Framework;

namespace test;

/// <summary>
/// Small set of test data, similar to the original TestData.
/// </summary>
internal static class TestData
{
    public static readonly Guid Guid1 = Guid.Parse("11111111-1111-1111-1111-111111111111");
    public static readonly Guid Guid2 = Guid.Parse("22222222-2222-2222-2222-222222222222");

    // Arbitrary but fixed UTC date.
    public static readonly DateTime Date = new DateTime(2020, 01, 02, 03, 04, 05, DateTimeKind.Utc);
}

/// <summary>
/// Set of type tests for YDB. Logic is based on the original YdbTypeTests,
/// but without the context parameter and without IncludeDataSourcesAttribute.
/// </summary>
[TestFixture]
public sealed class YdbTypeTests : TypeTestsBase
{
    private static readonly string TestEscapingString =
        string.Join("", Enumerable.Range(0, 255).Select(i => (char)i));

    // Helper for integer mappings
    private ValueTask TestInteger<TType>(
        DataType dataType,
        TType min,
        TType max,
        bool? testParameters = null)
        where TType : struct
    {
        return TestInteger(
            new DbDataType(typeof(TType), dataType),
            min,
            max,
            testParameters);
    }

    private async ValueTask TestInteger<TType>(
        DbDataType dataType,
        TType min,
        TType max,
        bool? testParameters = null)
        where TType : struct
    {
        await TestType<TType, TType?>(
            dataType,
            default,
            default,
            testParameters: testParameters);

        await TestType<TType, TType?>(
            dataType,
            min,
            max,
            testParameters: testParameters);

        await TestType<TType, TType?>(
            dataType,
            max,
            min,
            testParameters: testParameters);
    }

    [Test]
    public async ValueTask TestBool()
    {
        await TestType<bool, bool?>(
            new DbDataType(typeof(bool)),
            default,
            default);

        await TestType<bool, bool?>(
            new DbDataType(typeof(bool)),
            true,
            false);

        await TestType<bool, bool?>(
            new DbDataType(typeof(bool)),
            false,
            true);
    }

    [Test]
    public async ValueTask TestInt8()
    {
        // default
        await TestType<sbyte, sbyte?>(
            new DbDataType(typeof(sbyte)),
            default,
            default);

        await TestType<sbyte, sbyte?>(
            new DbDataType(typeof(sbyte)),
            sbyte.MinValue,
            sbyte.MaxValue);

        await TestType<sbyte, sbyte?>(
            new DbDataType(typeof(sbyte)),
            sbyte.MaxValue,
            sbyte.MinValue);

        // other types: unsigned
        await TestInteger<byte> (DataType.SByte, 0, 127);
        await TestInteger<ushort>(DataType.SByte, 0, 127);
        await TestInteger<uint>  (DataType.SByte, 0, 127);
        await TestInteger<ulong> (DataType.SByte, 0, 127);

        // other types: signed
        await TestInteger<short>  (DataType.SByte, sbyte.MinValue, sbyte.MaxValue);
        await TestInteger<int>    (DataType.SByte, sbyte.MinValue, sbyte.MaxValue);
        await TestInteger<long>   (DataType.SByte, sbyte.MinValue, sbyte.MaxValue);
        await TestInteger<decimal>(DataType.SByte, sbyte.MinValue, sbyte.MaxValue);
        await TestInteger<float>  (DataType.SByte, sbyte.MinValue, sbyte.MaxValue);
        await TestInteger<double> (DataType.SByte, sbyte.MinValue, sbyte.MaxValue);
    }

    [Test]
    public async ValueTask TestUInt8()
    {
        // default
        await TestType<byte, sbyte?>(
            new DbDataType(typeof(byte)),
            default,
            default);

        await TestType<byte, byte?>(
            new DbDataType(typeof(byte)),
            byte.MinValue,
            byte.MaxValue);

        await TestType<byte, byte?>(
            new DbDataType(typeof(byte)),
            byte.MaxValue,
            byte.MinValue);

        // bool
        await TestInteger<bool>(DataType.Byte, false, true);

        // other types: unsigned
        await TestInteger<ushort>(DataType.Byte, byte.MinValue, byte.MaxValue);
        await TestInteger<uint>  (DataType.Byte, byte.MinValue, byte.MaxValue);
        await TestInteger<ulong> (DataType.Byte, byte.MinValue, byte.MaxValue);

        // other types: signed
        await TestInteger<sbyte> (DataType.Byte, 0, sbyte.MaxValue);
        await TestInteger<short> (DataType.Byte, byte.MinValue, byte.MaxValue);
        await TestInteger<int>   (DataType.Byte, byte.MinValue, byte.MaxValue);
        await TestInteger<long>  (DataType.Byte, byte.MinValue, byte.MaxValue);
        await TestInteger<decimal>(DataType.Byte, byte.MinValue, byte.MaxValue);
        await TestInteger<float> (DataType.Byte, byte.MinValue, byte.MaxValue);
        await TestInteger<double>(DataType.Byte, byte.MinValue, byte.MaxValue);
    }

    [Test]
    public async ValueTask TestInt16()
    {
        // default
        await TestType<short, short?>(
            new DbDataType(typeof(short)),
            default,
            default);

        await TestType<short, short?>(
            new DbDataType(typeof(short)),
            short.MinValue,
            short.MaxValue);

        await TestType<short, short?>(
            new DbDataType(typeof(short)),
            short.MaxValue,
            short.MinValue);

        // other types: unsigned
        await TestInteger<byte> (DataType.Int16, 0, byte.MaxValue);
        await TestInteger<ushort>(DataType.Int16, 0, (ushort)short.MaxValue);
        await TestInteger<uint>  (DataType.Int16, 0, (uint)short.MaxValue);
        await TestInteger<ulong> (DataType.Int16, 0, (ulong)short.MaxValue);

        // other types: signed
        await TestInteger<sbyte> (DataType.Int16, sbyte.MinValue, sbyte.MaxValue);
        await TestInteger<int>   (DataType.Int16, short.MinValue, short.MaxValue);
        await TestInteger<long>  (DataType.Int16, short.MinValue, short.MaxValue);
        await TestInteger<decimal>(DataType.Int16, short.MinValue, short.MaxValue);
        await TestInteger<float> (DataType.Int16, short.MinValue, short.MaxValue);
        await TestInteger<double>(DataType.Int16, short.MinValue, short.MaxValue);
    }

    [Test]
    public async ValueTask TestUInt16()
    {
        // default
        await TestType<ushort, ushort?>(
            new DbDataType(typeof(ushort)),
            default,
            default);

        await TestType<ushort, ushort?>(
            new DbDataType(typeof(ushort)),
            ushort.MinValue,
            ushort.MaxValue);

        await TestType<ushort, ushort?>(
            new DbDataType(typeof(ushort)),
            ushort.MaxValue,
            ushort.MinValue);

        // other types: unsigned
        await TestInteger<byte> (DataType.UInt16, byte.MinValue, byte.MaxValue);
        await TestInteger<uint> (DataType.UInt16, ushort.MinValue, ushort.MaxValue);
        await TestInteger<ulong>(DataType.UInt16, ushort.MinValue, ushort.MaxValue);

        // other types: signed
        await TestInteger<sbyte> (DataType.UInt16, 0, sbyte.MaxValue);
        await TestInteger<short> (DataType.UInt16, 0, short.MaxValue);
        await TestInteger<int>   (DataType.UInt16, ushort.MinValue, ushort.MaxValue);
        await TestInteger<long>  (DataType.UInt16, ushort.MinValue, ushort.MaxValue);
        await TestInteger<decimal>(DataType.UInt16, ushort.MinValue, ushort.MaxValue);
        await TestInteger<float> (DataType.UInt16, ushort.MinValue, ushort.MaxValue);
        await TestInteger<double>(DataType.UInt16, ushort.MinValue, ushort.MaxValue);
    }

    [Test]
    public async ValueTask TestInt32()
    {
        // default
        await TestType<int, int?>(
            new DbDataType(typeof(int)),
            default,
            default);

        await TestType<int, int?>(
            new DbDataType(typeof(int)),
            int.MinValue,
            int.MaxValue);

        await TestType<int, int?>(
            new DbDataType(typeof(int)),
            int.MaxValue,
            int.MinValue);

        // other types: unsigned
        await TestInteger<byte> (DataType.Int32, 0, byte.MaxValue);
        await TestInteger<ushort>(DataType.Int32, 0, ushort.MaxValue);
        await TestInteger<uint>  (DataType.Int32, 0, (uint)int.MaxValue);
        await TestInteger<ulong> (DataType.Int32, 0, (ulong)int.MaxValue);

        // other types: signed
        await TestInteger<sbyte> (DataType.Int32, sbyte.MinValue, sbyte.MaxValue);
        await TestInteger<short> (DataType.Int32, short.MinValue, short.MaxValue);
        await TestInteger<long>  (DataType.Int32, int.MinValue, int.MaxValue);
        await TestInteger<decimal>(DataType.Int32, int.MinValue, int.MaxValue);
        await TestInteger<float> (DataType.Int32, 16777216, 16777216);
        await TestInteger<double>(DataType.Int32, int.MinValue, int.MaxValue);
    }

    [Test]
    public async ValueTask TestUInt32()
    {
        // default
        await TestType<uint, uint?>(
            new DbDataType(typeof(uint)),
            default,
            default);

        await TestType<uint, uint?>(
            new DbDataType(typeof(uint)),
            uint.MinValue,
            uint.MaxValue);

        await TestType<uint, uint?>(
            new DbDataType(typeof(uint)),
            uint.MaxValue,
            uint.MinValue);

        // other types: unsigned
        await TestInteger<byte> (DataType.UInt32, byte.MinValue, byte.MaxValue);
        await TestInteger<ushort>(DataType.UInt32, ushort.MinValue, ushort.MaxValue);
        await TestInteger<ulong>(DataType.UInt32, uint.MinValue, uint.MaxValue);

        // other types: signed
        await TestInteger<sbyte> (DataType.UInt32, 0, sbyte.MaxValue);
        await TestInteger<short> (DataType.UInt32, 0, short.MaxValue);
        await TestInteger<int>   (DataType.UInt32, 0, int.MaxValue);
        await TestInteger<long>  (DataType.UInt32, uint.MinValue, uint.MaxValue);
        await TestInteger<decimal>(DataType.UInt32, uint.MinValue, uint.MaxValue);
        await TestInteger<float> (DataType.UInt32, uint.MinValue, 16777216u);
        await TestInteger<double>(DataType.UInt32, uint.MinValue, uint.MaxValue);
    }

    [Test]
    public async ValueTask TestInt64()
    {
        // default
        await TestType<long, long?>(
            new DbDataType(typeof(long)),
            default,
            default);

        await TestType<long, long?>(
            new DbDataType(typeof(long)),
            long.MinValue,
            long.MaxValue);

        await TestType<long, long?>(
            new DbDataType(typeof(long)),
            long.MaxValue,
            long.MinValue);

        // other types: unsigned
        await TestInteger<byte> (DataType.Int64, 0, byte.MaxValue);
        await TestInteger<ushort>(DataType.Int64, 0, ushort.MaxValue);
        await TestInteger<uint>  (DataType.Int64, 0, uint.MaxValue);
        await TestInteger<ulong> (DataType.Int64, 0, (ulong)long.MaxValue);

        // other types: signed
        await TestInteger<sbyte> (DataType.Int64, sbyte.MinValue, sbyte.MaxValue);
        await TestInteger<short> (DataType.Int64, short.MinValue, short.MaxValue);
        await TestInteger<int>   (DataType.Int64, int.MinValue, int.MaxValue);
        await TestInteger<decimal>(DataType.Int64, long.MinValue, long.MaxValue);
        await TestInteger<float> (DataType.Int64, -16777216L, 16777216L);
        await TestInteger<double>(DataType.Int64, -9007199254740991L, 9007199254740991L);
    }

    [Test]
    public async ValueTask TestUInt64()
    {
        // default
        await TestType<ulong, ulong?>(
            new DbDataType(typeof(ulong)),
            default,
            default);

        await TestType<ulong, ulong?>(
            new DbDataType(typeof(ulong)),
            ulong.MinValue,
            ulong.MaxValue);

        await TestType<ulong, ulong?>(
            new DbDataType(typeof(ulong)),
            ulong.MaxValue,
            ulong.MinValue);

        // other types: unsigned
        await TestInteger<byte> (DataType.UInt64, byte.MinValue, byte.MaxValue);
        await TestInteger<ushort>(DataType.UInt64, ushort.MinValue, ushort.MaxValue);
        await TestInteger<uint> (DataType.UInt64, uint.MinValue, uint.MaxValue);

        // other types: signed
        await TestInteger<sbyte> (DataType.UInt64, 0, sbyte.MaxValue);
        await TestInteger<short> (DataType.UInt64, 0, short.MaxValue);
        await TestInteger<int>   (DataType.UInt64, 0, int.MaxValue);
        await TestInteger<long>  (DataType.UInt64, 0, long.MaxValue);
        await TestInteger<decimal>(DataType.UInt64, ulong.MinValue, ulong.MaxValue);
        await TestInteger<float> (DataType.UInt64, ulong.MinValue, 16777216UL);
        await TestInteger<double>(DataType.UInt64, ulong.MinValue, 9007199254740991UL);
    }

    [Test]
    public async ValueTask TestFloat()
    {
        // default
        await TestType<float, float?>(
            new DbDataType(typeof(float)),
            default,
            default);

        await TestType<float, float?>(
            new DbDataType(typeof(float)),
            float.MinValue,
            float.MaxValue);

        await TestType<float, float?>(
            new DbDataType(typeof(float)),
            float.MaxValue,
            float.MinValue);

        await TestType<float, float?>(
            new DbDataType(typeof(float)),
            float.Epsilon,
            float.NaN,
            filterByNullableValue: false);

        await TestType<float, float?>(
            new DbDataType(typeof(float)),
            float.NaN,
            float.Epsilon,
            filterByValue: false);

        await TestType<float, float?>(
            new DbDataType(typeof(float)),
            float.PositiveInfinity,
            float.NegativeInfinity);

        await TestType<float, float?>(
            new DbDataType(typeof(float)),
            float.NegativeInfinity,
            float.PositiveInfinity);

        // other types: unsigned
        await TestInteger<byte> (DataType.Single, 0, byte.MaxValue);
        await TestInteger<ushort>(DataType.Single, 0, ushort.MaxValue);
        await TestInteger<uint>  (DataType.Single, 0, 16777216u);
        await TestInteger<ulong> (DataType.Single, 0, 16777216ul);

        // other types: signed
        await TestInteger<sbyte> (DataType.Single, sbyte.MinValue, sbyte.MaxValue);
        await TestInteger<short> (DataType.Single, short.MinValue, short.MaxValue);
        await TestInteger<int>   (DataType.Single, -16777216, 16777216);
        await TestInteger<long>  (DataType.Single, -16777216L, 16777216L);
        await TestInteger<decimal>(DataType.Single, -16777220m, 16777220m);
        await TestInteger<double>(DataType.Single, float.MinValue, float.MaxValue);
    }

    [Test]
    public async ValueTask TestDouble()
    {
        // default
        await TestType<double, double?>(
            new DbDataType(typeof(double)),
            default,
            default);

        await TestType<double, double?>(
            new DbDataType(typeof(double)),
            double.MinValue,
            double.MaxValue);

        await TestType<double, double?>(
            new DbDataType(typeof(double)),
            double.MaxValue,
            double.MinValue);

        await TestType<double, double?>(
            new DbDataType(typeof(double)),
            double.Epsilon,
            double.NaN,
            filterByNullableValue: false);

        await TestType<double, double?>(
            new DbDataType(typeof(double)),
            double.NaN,
            double.Epsilon,
            filterByValue: false);

        await TestType<double, double?>(
            new DbDataType(typeof(double)),
            double.PositiveInfinity,
            double.NegativeInfinity);

        await TestType<double, double?>(
            new DbDataType(typeof(double)),
            double.NegativeInfinity,
            double.PositiveInfinity);

        // other types: unsigned
        await TestInteger<byte> (DataType.Double, 0, byte.MaxValue);
        await TestInteger<ushort>(DataType.Double, 0, ushort.MaxValue);
        await TestInteger<uint>  (DataType.Double, 0, uint.MaxValue);
        await TestInteger<ulong> (DataType.Double, 0, 9007199254740991UL);

        // other types: signed
        await TestInteger<sbyte> (DataType.Double, sbyte.MinValue, sbyte.MaxValue);
        await TestInteger<short> (DataType.Double, short.MinValue, short.MaxValue);
        await TestInteger<int>   (DataType.Double, int.MinValue, int.MaxValue);
        await TestInteger<long>  (DataType.Double, -9007199254740991L, 9007199254740991L);
        await TestInteger<decimal>(DataType.Double, -9007199254740990m, 9007199254740990m);
        await TestInteger<float> (DataType.Double, float.MinValue, float.MaxValue);
    }

    [Test]
    public async ValueTask TestDecimal()
    {
        // default mapping
        var defaultMax = 6251426433751.935439503M;
        var defaultMin = -6251426433752.935439503M;

        await TestType<decimal, decimal?>(
            new DbDataType(typeof(decimal)),
            default,
            default);

        await TestType<decimal, decimal?>(
            new DbDataType(typeof(decimal)),
            defaultMax,
            defaultMin);

        var precisions = new[] { 1, 2, 21, 22, 23, 34, 35 };

        foreach (var p in precisions)
        {
            for (var s = 0; s <= p; s++)
            {
                // test only s=0,1,..,p-1, p and 9
                if (s > 1 && s < p - 1 && s != 9)
                    continue;

                var decimalType = new DbDataType(typeof(decimal))
                    .WithPrecision(p)
                    .WithScale(s);

                var stringType = new DbDataType(typeof(string), DataType.Decimal, null, null, p, s);

                var maxString = new string('9', p);

                if (s > 0)
                    maxString = maxString.Substring(0, p - s) + "." + maxString.Substring(p - s);

                if (maxString[0] == '.')
                    maxString = "0" + maxString;

                var minString = "-" + maxString;

                decimal minDecimal;
                decimal maxDecimal;

                if (p >= 29)
                {
                    maxDecimal = decimal.MaxValue;
                    minDecimal = decimal.MinValue;

                    for (var i = 0; i < s; i++)
                    {
                        maxDecimal /= 10;
                        minDecimal /= 10;
                    }
                }
                else
                {
                    maxDecimal = decimal.Parse(maxString, CultureInfo.InvariantCulture);
                    minDecimal = -maxDecimal;
                }

                // another provider bug - cannot read big decimals
                if (p < 34)
                {
                    await TestType<decimal, decimal?>(
                        decimalType,
                        default,
                        default);

                    await TestType<decimal, decimal?>(
                        decimalType,
                        minDecimal,
                        maxDecimal);

                    var zero = s == 0
                        ? "0"
                        : "0." + new string('0', s);

                    await TestType<string, string?>(
                        stringType,
                        "0",
                        default,
                        getExpectedValue: _ => zero);

                    await TestType<string, string?>(
                        stringType,
                        minString,
                        maxString);

                    if (s > 0 && p > s)
                    {
                        await TestType<string, string?>(
                            stringType,
                            "1.2",
                            "-2.1",
                            getExpectedValue: v => v + new string('0', s - 1),
                            getExpectedNullableValue: v => v + new string('0', s - 1));
                    }
                }
            }
        }

        // unsigned
        await TestInteger<byte> (DataType.Decimal, 0, byte.MaxValue);
        await TestInteger<ushort>(DataType.Decimal, 0, ushort.MaxValue);
        await TestInteger<uint>  (DataType.Decimal, 0, uint.MaxValue);

        await TestInteger<ulong>(
            new DbDataType(typeof(ulong), DataType.Decimal).WithPrecision(20).WithScale(0),
            0,
            ulong.MaxValue);

        // signed
        await TestInteger<sbyte> (DataType.Decimal, sbyte.MinValue, sbyte.MaxValue);
        await TestInteger<short> (DataType.Decimal, short.MinValue, short.MaxValue);
        await TestInteger<int>   (DataType.Decimal, int.MinValue, int.MaxValue);

        await TestInteger<long>(
            new DbDataType(typeof(long), DataType.Decimal).WithPrecision(19).WithScale(0),
            long.MinValue,
            long.MaxValue);

        await TestInteger<float>(
            new DbDataType(typeof(float), DataType.Decimal).WithPrecision(8).WithScale(0),
            -16777220L,
            16777220L);

        await TestInteger<double>(
            new DbDataType(typeof(double), DataType.Decimal).WithPrecision(16).WithScale(0),
            -9007199254740990L,
            9007199254740990L);
    }

    [Test]
    public async ValueTask TestString()
    {
        // ---------- byte[] <-> Binary (by value) ----------

        await TestType<byte[], byte[]?>(
            new DbDataType(typeof(byte[])),
            Array.Empty<byte>(),
            default);

        await TestType<byte[], byte[]?>(
            new DbDataType(typeof(byte[])),
            new byte[] { 0, 1, 2, 3, 4, 0 },
            new byte[] { 1, 2, 3, 4, 0, 0 });

        // ---------- string <-> VarBinary ----------
        // String representation for Binary/VarBinary depends on the provider
        // (custom encoding in YDB provider), so we don't compare values,
        // we only check that data can be read without errors.

        await TestType<string, string?>(
            new DbDataType(typeof(string), DataType.VarBinary),
            string.Empty,
            default,
            filterByValue: false,
            filterByNullableValue: false,
            isExpectedValue: _ => true,
            isExpectedNullableValue: _ => true);

        await TestType<string, string?>(
            new DbDataType(typeof(string), DataType.VarBinary),
            TestEscapingString,
            TestEscapingString,
            filterByValue: false,
            filterByNullableValue: false,
            isExpectedValue: _ => true,
            isExpectedNullableValue: _ => true);

        // ---------- string <-> Binary ----------

        await TestType<string, string?>(
            new DbDataType(typeof(string), DataType.Binary),
            string.Empty,
            default,
            filterByValue: false,
            filterByNullableValue: false,
            isExpectedValue: _ => true,
            isExpectedNullableValue: _ => true);

        await TestType<string, string?>(
            new DbDataType(typeof(string), DataType.Binary),
            TestEscapingString,
            TestEscapingString,
            filterByValue: false,
            filterByNullableValue: false,
            isExpectedValue: _ => true,
            isExpectedNullableValue: _ => true);

        // ---------- MemoryStream <-> Binary ----------

        var streamEmpty = new MemoryStream(Array.Empty<byte>());
        var streamData1 = new MemoryStream(new byte[] { 0, 1, 2, 3, 4, 0 });
        var streamData2 = new MemoryStream(new byte[] { 1, 2, 3, 4, 0, 0 });

        await TestType<MemoryStream, MemoryStream?>(
            new DbDataType(typeof(MemoryStream)),
            streamEmpty,
            default);

        await TestType<MemoryStream, MemoryStream?>(
            new DbDataType(typeof(MemoryStream)),
            streamData1,
            streamData2);
    }

    [Test]
    public async ValueTask TestUtf8()
    {
        await TestType<string, string?>(
            new DbDataType(typeof(string)),
            string.Empty,
            default);

        await TestType<string, string?>(
            new DbDataType(typeof(string)),
            TestEscapingString,
            TestEscapingString);

        await TestType<char, char?>(
            new DbDataType(typeof(string)),
            default,
            default);

        await TestType<char, char?>(
            new DbDataType(typeof(string)),
            '\0',
            '1');

        await TestType<char, char?>(
            new DbDataType(typeof(string)),
            'ы',
            '\xFE');

        await TestType<char, char?>(
            new DbDataType(typeof(string)),
            '\xFF',
            '\n');
    }

    [Test]
    public async ValueTask TestJson()
    {
        await TestType<string, string?>(
            new DbDataType(typeof(string), DataType.Json),
            "{}",
            default,
            filterByValue: false,
            filterByNullableValue: false);

        await TestType<string, string?>(
            new DbDataType(typeof(string), DataType.Json),
            "null",
            "null",
            filterByValue: false,
            filterByNullableValue: false);

        await TestType<string, string?>(
            new DbDataType(typeof(string), DataType.Json),
            "false",
            "true",
            filterByValue: false,
            filterByNullableValue: false);

        await TestType<string, string?>(
            new DbDataType(typeof(string), DataType.Json),
            "\"test\"",
            "123",
            filterByValue: false,
            filterByNullableValue: false);

        await TestType<string, string?>(
            new DbDataType(typeof(string), DataType.Json),
            "{\"ы\": 1.23}",
            "{\"prop\": false }",
            filterByValue: false,
            filterByNullableValue: false);
    }

    [Test]
    public async ValueTask TestJsonDocument()
    {
        await TestType<string, string?>(
            new DbDataType(typeof(string), DataType.BinaryJson),
            "{}",
            default,
            filterByValue: false,
            filterByNullableValue: false);

        await TestType<string, string?>(
            new DbDataType(typeof(string), DataType.BinaryJson),
            "null",
            "null",
            filterByValue: false,
            filterByNullableValue: false);

        await TestType<string, string?>(
            new DbDataType(typeof(string), DataType.BinaryJson),
            "false",
            "true",
            filterByValue: false,
            filterByNullableValue: false);

        await TestType<string, string?>(
            new DbDataType(typeof(string), DataType.BinaryJson),
            "12.34",
            "123",
            filterByValue: false,
            filterByNullableValue: false);

        await TestType<string, string?>(
            new DbDataType(typeof(string), DataType.BinaryJson),
            "{\"ы\":1.23}",
            "{\"prop\":false}",
            filterByValue: false,
            filterByNullableValue: false);
    }

    [Test]
    public async ValueTask TestUUID()
    {
        await TestType<Guid, Guid?>(
            new DbDataType(typeof(Guid)),
            default,
            default);

        await TestType<Guid, Guid?>(
            new DbDataType(typeof(Guid)),
            TestData.Guid1,
            TestData.Guid2);
    }

    [Test]
    public async ValueTask TestDateTime()
    {
        var min = new DateTime(1970, 1, 1);
        var max = new DateTime(2105, 12, 31);

        var minWithTime = min.AddSeconds(1);
        var maxWithTime = max.AddSeconds(-1);

        // Disable ProviderSpecific BulkCopy to avoid
        // "Type mismatch: Timestamp vs Datetime" on BulkUpsert.
        Func<BulkCopyType, bool> noProviderSpecific =
            t => t != BulkCopyType.ProviderSpecific;

        // DateTime → YDB Datetime (seconds precision)
        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime), DataType.DateTime),
            TestData.Date,
            null,
            testBulkCopyType: noProviderSpecific);

        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime), DataType.DateTime),
            min,
            max,
            testBulkCopyType: noProviderSpecific);

        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime), DataType.DateTime),
            minWithTime,
            maxWithTime,
            testBulkCopyType: noProviderSpecific);

        // DateTimeOffset → YDB Datetime
        await TestType<DateTimeOffset, DateTimeOffset?>(
            new DbDataType(typeof(DateTimeOffset), DataType.DateTime),
            new DateTimeOffset(TestData.Date, default),
            null,
            testBulkCopyType: noProviderSpecific);

        await TestType<DateTimeOffset, DateTimeOffset?>(
            new DbDataType(typeof(DateTimeOffset), DataType.DateTime),
            new DateTimeOffset(min, default),
            new DateTimeOffset(max, default),
            testBulkCopyType: noProviderSpecific);

        await TestType<DateTimeOffset, DateTimeOffset?>(
            new DbDataType(typeof(DateTimeOffset), DataType.DateTime),
            new DateTimeOffset(minWithTime, default),
            new DateTimeOffset(maxWithTime, default),
            testBulkCopyType: noProviderSpecific);
    }

    [Test]
    public async ValueTask TestTimestamp()
    {
        var min = new DateTime(1970, 1, 1);
        var max = new DateTime(2105, 12, 31);

        var minWithTime = min.AddTicks(10);
        var maxWithTime = max.AddTicks(-10);

        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime)),
            TestData.Date,
            default);

        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime)),
            min,
            max);

        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime)),
            minWithTime,
            maxWithTime);

        var minDto = new DateTimeOffset(min, default);
        var maxDto = new DateTimeOffset(max, default);

        await TestType<DateTimeOffset, DateTimeOffset?>(
            new DbDataType(typeof(DateTimeOffset)),
            new DateTimeOffset(TestData.Date),
            default);

        await TestType<DateTimeOffset, DateTimeOffset?>(
            new DbDataType(typeof(DateTimeOffset)),
            minDto,
            maxDto);

        await TestType<DateTimeOffset, DateTimeOffset?>(
            new DbDataType(typeof(DateTimeOffset)),
            maxDto,
            minDto);
    }

    [Test]
    public async ValueTask TestInterval()
    {
        var min = TimeSpan.FromDays(-49673) + TimeSpan.FromTicks(1);
        var max = TimeSpan.FromDays(49673) - TimeSpan.FromTicks(1);

        var minExpected = TimeSpan.FromDays(-49673) + TimeSpan.FromTicks(10);
        var maxExpected = TimeSpan.FromDays(49673) - TimeSpan.FromTicks(10);

        await TestType<TimeSpan, TimeSpan?>(
            new DbDataType(typeof(TimeSpan)),
            max,
            default,
            getExpectedValue: _ => maxExpected);

        await TestType<TimeSpan, TimeSpan?>(
            new DbDataType(typeof(TimeSpan)),
            min,
            max,
            getExpectedValue: _ => minExpected,
            getExpectedNullableValue: _ => maxExpected);
    }

    [Test]
    public async ValueTask TestDate()
    {
        var min = new DateTime(1970, 1, 1);
        var max = new DateTime(2105, 12, 31);

#if SUPPORTS_DATEONLY
            // DateOnly → YDB Date
            await TestType<DateOnly, DateOnly?>(
                new DbDataType(typeof(DateOnly)),
                DateOnly.FromDateTime(TestData.Date),
                null);

            await TestType<DateOnly, DateOnly?>(
                new DbDataType(typeof(DateOnly)),
                DateOnly.FromDateTime(min),
                DateOnly.FromDateTime(max));
#endif

        // YDB Date stores only date part, so we truncate time
        // component in expected value.
        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime), DataType.Date),
            TestData.Date,
            null,
            getExpectedValue: v => v.Date);

        // YDB returns Date as UTC date
        var expectedMin = new DateTime(min.Ticks, DateTimeKind.Utc);
        var expectedMax = new DateTime(max.Ticks, DateTimeKind.Utc);

        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime), DataType.Date),
            min,
            max,
            getExpectedValue: _ => expectedMin,
            getExpectedNullableValue: _ => expectedMax);

        var expectedDtoMin = new DateTimeOffset(min);
        var expectedDtoMax = new DateTimeOffset(max);

        // DateTimeOffset → Date: only date, without time
        await TestType<DateTimeOffset, DateTimeOffset?>(
            new DbDataType(typeof(DateTimeOffset), DataType.Date),
            new DateTimeOffset(max, default),
            null,
            getExpectedValue: _ => expectedDtoMax);

        await TestType<DateTimeOffset, DateTimeOffset?>(
            new DbDataType(typeof(DateTimeOffset), DataType.Date),
            new DateTimeOffset(min, default),
            new DateTimeOffset(max, default),
            getExpectedValue: _ => expectedDtoMin,
            getExpectedNullableValue: _ => expectedDtoMax);
    }

    [Test]
    public async ValueTask TestDate32()
    {
        var min = new DateTime(1970, 1, 1);
        var max = new DateTime(2105, 12, 31);

#if SUPPORTS_DATEONLY
            // DateOnly → YDB Date32 (same DataType.Date, but a new YDB type)
            await TestType<DateOnly, DateOnly?>(
                new DbDataType(typeof(DateOnly), DataType.Date),
                DateOnly.FromDateTime(TestData.Date),
                null);

            await TestType<DateOnly, DateOnly?>(
                new DbDataType(typeof(DateOnly), DataType.Date),
                DateOnly.FromDateTime(min),
                DateOnly.FromDateTime(max));
#endif

        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime), DataType.Date),
            TestData.Date,
            null,
            getExpectedValue: v => v.Date);

        // YDB returns Date as UTC date
        var expectedMin = new DateTime(min.Ticks, DateTimeKind.Utc);
        var expectedMax = new DateTime(max.Ticks, DateTimeKind.Utc);

        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime), DataType.Date),
            min,
            max,
            getExpectedValue: _ => expectedMin,
            getExpectedNullableValue: _ => expectedMax);
    }

    [Test]
    public async ValueTask TestDatetime64()
    {
        var min         = new DateTime(1970, 1, 1);
        var max         = new DateTime(2105, 12, 31);
        var minWithTime = min.AddTicks(10);
        var maxWithTime = max.AddTicks(-10);

        // DateTime → Timestamp (µs)
        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime), DataType.DateTime2),
            TestData.Date,
            default);

        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime), DataType.DateTime2),
            min,
            max);

        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime), DataType.DateTime2),
            minWithTime,
            maxWithTime);

        await TestType<DateTimeOffset, DateTimeOffset?>(
            new DbDataType(typeof(DateTimeOffset), DataType.DateTime2),
            new DateTimeOffset(TestData.Date, default),
            default,
            testParameters: false);

        await TestType<DateTimeOffset, DateTimeOffset?>(
            new DbDataType(typeof(DateTimeOffset), DataType.DateTime2),
            new DateTimeOffset(min, default),
            new DateTimeOffset(max, default),
            testParameters: false);

        await TestType<DateTimeOffset, DateTimeOffset?>(
            new DbDataType(typeof(DateTimeOffset), DataType.DateTime2),
            new DateTimeOffset(minWithTime, default),
            new DateTimeOffset(maxWithTime, default),
            testParameters: false);
    }

    [Test]
    public async ValueTask TestTimestamp64()
    {
        var min         = new DateTime(1970, 1, 1);
        var max         = new DateTime(2105, 12, 31);
        var minWithTime = min.AddTicks(10);
        var maxWithTime = max.AddTicks(-10);

        // DateTime → Timestamp (µs)
        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime), DataType.DateTime2),
            TestData.Date,
            default);

        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime), DataType.DateTime2),
            min,
            max);

        await TestType<DateTime, DateTime?>(
            new DbDataType(typeof(DateTime), DataType.DateTime2),
            minWithTime,
            maxWithTime);

        await TestType<DateTimeOffset, DateTimeOffset?>(
            new DbDataType(typeof(DateTimeOffset), DataType.DateTime2),
            new DateTimeOffset(TestData.Date, default),
            default,
            testParameters: false);

        await TestType<DateTimeOffset, DateTimeOffset?>(
            new DbDataType(typeof(DateTimeOffset), DataType.DateTime2),
            new DateTimeOffset(min, default),
            new DateTimeOffset(max, default),
            testParameters: false);

        await TestType<DateTimeOffset, DateTimeOffset?>(
            new DbDataType(typeof(DateTimeOffset), DataType.DateTime2),
            new DateTimeOffset(max, default),
            new DateTimeOffset(min, default),
            testParameters: false);
    }

    [Test]
    public async ValueTask TestInterval64()
    {
        var min = TimeSpan.FromDays(-49673) + TimeSpan.FromTicks(1);
        var max = TimeSpan.FromDays(49673)  - TimeSpan.FromTicks(1);

        // In YDB Interval is stored in microseconds: values are rounded to 10 ticks
        var minExpected = TimeSpan.FromDays(-49673) + TimeSpan.FromTicks(10);
        var maxExpected = TimeSpan.FromDays(49673)  - TimeSpan.FromTicks(10);

        await TestType<TimeSpan, TimeSpan?>(
            new DbDataType(typeof(TimeSpan), DataType.Interval),
            max,
            default,
            getExpectedValue: _ => maxExpected);

        await TestType<TimeSpan, TimeSpan?>(
            new DbDataType(typeof(TimeSpan), DataType.Interval),
            min,
            max,
            getExpectedValue: _ => minExpected,
            getExpectedNullableValue: _ => maxExpected);
    }
}