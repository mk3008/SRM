using InterlinkMapper.Data;

namespace InterlinkMapper.Tables;

public class HoldTable : DbTable
{
	public HoldTable(Datasource datasource)
	{
		if (datasource.Destination == null) throw new InvalidOperationException();
		if (datasource.Destination.Table == null) throw new InvalidOperationException();
		Datasource = datasource;
		TableName = datasource.Destination.Table.TableName + "__hld_" + datasource.DatasourceName;
	}

	public Datasource Datasource { get; init; }

	public override IEnumerable<string> GetUniqueKeyColumns()
	{
		yield break;
	}

	public override IEnumerable<string> GetPrimaryKeyColumns()
	{
		return GetColumns();
	}

	public override IEnumerable<string> GetColumns()
	{
		foreach (var key in Datasource.KeyColumns) yield return key;
	}

	public override string? GetSequenceColumn()
	{
		return null;
	}
}
