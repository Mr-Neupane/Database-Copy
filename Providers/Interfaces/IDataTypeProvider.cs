using NpgsqlTypes;

namespace Database_Copy.Providers.Interfaces;

public interface IDataTypeProvider
{
    string GetCompatibleDataTypeForMssql(string dataType);
    string GetCompatibleDataTypeForPsql(string dataType);
    NpgsqlDbType GetTypeForPsql(Type dataType);
}