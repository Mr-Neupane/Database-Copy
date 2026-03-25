namespace Database_Copy.Models;

public class Schema
{
    public string OldSchemaName { get; set; }
    public string NewSchemaName => string.Concat(OldSchemaName, "_mig");
}

public class DbTable : Schema
{
    public string TableName { get; set; }
}

public class Columns : DbTable
{
    public string ColumnName { get; set; }
    public string DataType { get; set; }
    public string IsNullable { get; set; }
    public string IsIdentity { get; set; }
}