namespace Database_Copy.Models;

public class Schema
{
    public string OldSchemaName { get; set; }
    public string NewSchemaName => string.Concat(OldSchemaName, "_mig");
}

public class Table : Schema
{
    public string TableName { get; set; }
}

public class Columns : Table
{
    public string ColumnName { get; set; }
    public string DataType { get; set; }
    public string IsNullable { get; set; }
    public string IsIdentity { get; set; }
}