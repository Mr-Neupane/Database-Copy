namespace Database_Copy.Services.Interfaces;

public interface IDbCopyService
{
    void ValidateAndMigrate(string dbName, bool isToPostgres);
}