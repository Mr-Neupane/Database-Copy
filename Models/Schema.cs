namespace Database_Copy.Models;

public class Schema : IEquatable<Schema>
{
    public string OldSchemaName { get; set; }
    public string NewSchemaName => string.Concat(OldSchemaName, "_mig");

    public bool Equals(Schema? other)
    {
        if (other is null) return false;
        return OldSchemaName == other.OldSchemaName;
    }

    public override bool Equals(object? obj) => Equals(obj as Schema);

    public override int GetHashCode() => OldSchemaName?.GetHashCode() ?? 0;
}

public class DbTable : Schema, IEquatable<DbTable>
{
    public string TableName { get; set; }

    public bool Equals(DbTable? other)
    {
        if (other is null) return false;
        return base.Equals(other) && TableName == other.TableName;
    }

    public override bool Equals(object? obj) => Equals(obj as DbTable);

    public override int GetHashCode() => HashCode.Combine(OldSchemaName, TableName);
}

public class Columns : DbTable
{
    public string ColumnName { get; set; }
    public string DataType { get; set; }
    public string IsNullable { get; set; }
    public string IsIdentity { get; set; }
}