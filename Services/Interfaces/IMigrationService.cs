using Database_Copy.Models;

namespace Database_Copy.Services.Interfaces;

public interface IMigrationService
{
    void MigrateTableData(string dbName, List<DbTable> tables, bool isDesPsql = false);
    void DowngradeMssqlDb(string dbName, string queryPath);
}