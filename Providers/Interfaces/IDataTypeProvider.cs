namespace Database_Copy.Providers.Interfaces;

public interface IDataTypeProvider
{
    string GetCompatibleColumnTypeForMssql(string dataType);

}