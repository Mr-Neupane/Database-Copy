namespace Database_Copy.Services.Interfaces;

public interface IDbCopyService
{
    void ValidateAndCopy(string dbName, bool isToPostgres);
}