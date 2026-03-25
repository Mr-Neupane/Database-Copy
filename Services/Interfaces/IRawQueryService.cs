using System.Data;

namespace Database_Copy.Services.Interfaces;

public interface IRawQueryService
{
    void ExecuteQuery(IDbConnection dbConnection, string queryPath);
}