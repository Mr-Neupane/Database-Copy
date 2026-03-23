using Database_Copy.Providers.Interfaces;
using NpgsqlTypes;

namespace Database_Copy.Providers;

public class DataTypeProvider : IDataTypeProvider
{
    public string GetCompatibleDataTypeForMssql(string dataType)
    {
        if (string.IsNullOrWhiteSpace(dataType))
            return "NVARCHAR(MAX)";

        var normalizedType = dataType.Trim().ToLowerInvariant().Trim('"');

        return MapPsqlDataTypeToMssql.TryGetValue(normalizedType, out var sqlType)
            ? sqlType
            : "NVARCHAR(MAX)";
    }

    private static readonly Dictionary<string, string> MapPsqlDataTypeToMssql = new()
    {
        { "char", "CHAR(1)" },
        { "array", "NVARCHAR(MAX)" },
        { "anyarray", "NVARCHAR(MAX)" },
        { "bigint", "BIGINT" },
        { "boolean", "BIT" },
        { "bytea", "VARBINARY(MAX)" },
        { "character", "CHAR" },
        { "character varying", "NVARCHAR(MAX)" },
        { "date", "DATE" },
        { "double precision", "FLOAT" },
        { "inet", "NVARCHAR(45)" },
        { "integer", "INT" },
        { "interval", "NVARCHAR(50)" },
        { "jsonb", "NVARCHAR(MAX)" },
        { "name", "NVARCHAR(128)" },
        { "numeric", "DECIMAL" },
        { "oid", "INT" },
        { "pg_dependencies", "NVARCHAR(MAX)" },
        { "pg_lsn", "NVARCHAR(50)" },
        { "pg_mcv_list", "NVARCHAR(MAX)" },
        { "pg_ndistinct", "FLOAT" },
        { "pg_node_tree", "NVARCHAR(MAX)" },
        { "real", "REAL" },
        { "regproc", "NVARCHAR(256)" },
        { "regtype", "NVARCHAR(256)" },
        { "smallint", "SMALLINT" },
        { "text", "NVARCHAR(MAX)" },
        { "timestamp with time zone", "DATETIMEOFFSET" },
        { "timestamp without time zone", "DATETIME2" },
        { "uuid", "UNIQUEIDENTIFIER" },
        { "xid", "INT" }
    };

    private static readonly Dictionary<string, string> MapMssqlDataTypeToPsql = new()
    {
        { "bigint", "bigint" },
        { "binary", "bytea" },
        { "bit", "boolean" },
        { "char", "text" },
        { "date", "timestamp without time zone" },
        { "datetime", "timestamp without time zone" },
        { "datetime2", "timestamp without time zone" },
        { "datetimeoffset", "timestamp with time zone" },
        { "decimal", "numeric" },
        { "float", "double precision" },
        { "image", "bytea" },
        { "int", "integer" },
        { "money", "numeric(19,4)" },
        { "nchar", "char" },
        { "ntext", "text" },
        { "numeric", "numeric" },
        { "nvarchar", "text" },
        { "real", "real" },
        { "smalldatetime", "timestamp without time zone" },
        { "smallint", "smallint" },
        { "smallmoney", "numeric(10,4)" },
        { "text", "text" },
        { "time", "time without time zone" },
        { "timestamp", "bytea" }, // SQL Server timestamp = rowversion
        { "tinyint", "smallint" },
        { "uniqueidentifier", "uuid" },
        { "varbinary", "bytea" },
        { "varchar", "character varying" },
        { "xml", "xml" }
    };

    public string GetCompatibleDataTypeForPsql(string mssqlType)
    {
        var type = mssqlType.Trim().ToLower();

        var baseType = type.Contains("(")
            ? type.Substring(0, type.IndexOf("("))
            : type;

        if (MapMssqlDataTypeToPsql.TryGetValue(baseType, out var mapped))
        {
            if (type.Contains("max") && (baseType == "nvarchar" || baseType == "varchar"))
                return "text";

            return mapped;
        }

        if (type.StartsWith("nvarchar") || type.StartsWith("varchar"))
            return "text";

        if (type.StartsWith("nchar") || type.StartsWith("char"))
            return "text";

        if (type.StartsWith("decimal") || type.StartsWith("numeric"))
            return "numeric";

        if (type.StartsWith("varbinary") || type.StartsWith("binary"))
            return "bytea";

        return "text";
    }

    public NpgsqlDbType GetTypeForPsql(Type type)
    {
        if (type == typeof(byte)) return NpgsqlDbType.Smallint;

        if (type == typeof(short)) return NpgsqlDbType.Smallint;
        if (type == typeof(int)) return NpgsqlDbType.Integer;
        if (type == typeof(long)) return NpgsqlDbType.Bigint;

        if (type == typeof(float)) return NpgsqlDbType.Real;
        if (type == typeof(double)) return NpgsqlDbType.Double;
        if (type == typeof(decimal)) return NpgsqlDbType.Numeric;

        if (type == typeof(bool)) return NpgsqlDbType.Boolean;

        if (type == typeof(string)) return NpgsqlDbType.Text;
        if (type == typeof(char)) return NpgsqlDbType.Char;
        if (type == typeof(char[])) return NpgsqlDbType.Text;

        if (type == typeof(DateTime)) return NpgsqlDbType.Timestamp;
        if (type == typeof(DateTimeOffset)) return NpgsqlDbType.TimestampTz;

        if (type == typeof(TimeSpan)) return NpgsqlDbType.Interval;

        if (type == typeof(Guid)) return NpgsqlDbType.Uuid;

        if (type == typeof(byte[])) return NpgsqlDbType.Bytea;

        var underlyingType = Nullable.GetUnderlyingType(type);
        if (underlyingType != null)
            return GetTypeForPsql(underlyingType);
        throw new NotSupportedException($"Type {type} not supported");
    }
}