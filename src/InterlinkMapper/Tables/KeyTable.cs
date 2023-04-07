using InterlinkMapper.Data;

namespace InterlinkMapper.Tables;

public class KeyTable : DbTable
{
	public KeyTable(Datasource datasource)
	{
		if (datasource.Destination == null) throw new InvalidOperationException();
		if (datasource.Destination.Table == null) throw new InvalidOperationException();
		Datasource = datasource;
		TableName = datasource.Destination.Table.TableName + "__key_" + datasource.DatasourceName;
	}

	public Datasource Datasource { get; init; }

	public override IEnumerable<string> GetUniqueKeyColumns()
	{
		if (Datasource.Destination == null) yield break;
		if (Datasource.Destination.Sequence == null) yield break;

		yield return Datasource.Destination.Sequence.Column;
	}

	public override IEnumerable<string> GetPrimaryKeyColumns()
	{
		foreach (var key in Datasource.KeyColumns) yield return key;
	}

	public override IEnumerable<string> GetColumns()
	{
		if (Datasource.Destination == null) yield break;
		if (Datasource.Destination.Sequence == null) yield break;

		yield return Datasource.Destination.Sequence.Column;
		foreach (var key in Datasource.KeyColumns) yield return key;
	}

	public override string? GetSequenceColumn()
	{
		return null;
	}
}
