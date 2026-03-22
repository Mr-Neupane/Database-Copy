using Database_Copy.Providers.Interfaces;

namespace Database_Copy.Providers;

public class DataTypeProvider : IDataTypeProvider
{
    public string GetCompatibleColumnTypeForMssql(string dataType)
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
}