using InterlinkMapper.Materializer;
using PropertyBind;
using RedOrb;
using RedOrb.Attributes;

namespace InterlinkMapper.Models;

[GeneratePropertyBind(nameof(Datasources), nameof(InterlinkDatasource.Destination))]
[DbTable]
public partial class InterlinkDestination
{
	[DbColumn("numeric", IsAutoNumber = true, IsPrimaryKey = true)]
	public long InterlinkDestinationId { get; set; }

	[DbColumn("text")]
	public required string TableFullName { get; set; }

	[DbColumn("text")]
	public required DbTable DbTable { get; set; }

	[DbColumn("text")]
	public required Sequence DbSequence { get; set; }

	[DbColumn("text")]
	public string Description { get; set; } = string.Empty;

	[DbColumn("text")]
	public required ReverseOption ReverseOption { get; set; }

	[DbColumn("timestamp", SpecialColumn = SpecialColumn.CreateTimestamp)]
	public DateTime CreatedAt { get; set; }

	[DbColumn("timestamp", SpecialColumn = SpecialColumn.UpdateTimestamp)]
	public DateTime UpdatedAt { get; set; }

	[DbColumn("numeric", SpecialColumn = SpecialColumn.VersionNumber)]
	public long LockVersion { get; set; }

	public bool AllowReverse => ReverseOption.ReverseColumns.Any();

	[DbChildren]
	public DirtyCheckableCollection<InterlinkDatasource> Datasources { get; }

	public InterlinkRelationTable GetInterlinkRelationTable(SystemEnvironment env)
	{
		var rootColumn = string.Format(env.DbTableConfig.RootIdColumnFormat, DbSequence.ColumnName);
		var originColumn = string.Format(env.DbTableConfig.OriginIdColumnFormat, DbSequence.ColumnName);

		var proc = ObjectRelationMapper.FindFirst<InterlinkProcess>();
		var procId = proc.ColumnDefinitions.Where(x => x.Identifer == nameof(InterlinkProcess.InterlinkProcessId)).First();

		var t = new InterlinkRelationTable()
		{
			Definition = new()
			{
				SchemaName = env.DbTableConfig.ControlTableSchemaName,
				TableName = string.Format(env.DbTableConfig.RelationTableNameFormat, DbTable.TableName),
				ColumnContainers = new()
				{
					new DbColumnDefinition()
					{
						ColumnName = DbSequence.ColumnName,
						ColumnType = env.DbEnvironment.NumericTypeName,
						IsNullable= false,
						IsPrimaryKey = true,
					},
					new DbColumnDefinition()
					{
						Identifer = procId.Identifer,
						ColumnName = procId.ColumnName,
						ColumnType = env.DbEnvironment.NumericTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = rootColumn,
						ColumnType = env.DbEnvironment.NumericTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = originColumn,
						ColumnType = env.DbEnvironment.NumericTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = env.DbTableConfig.RemarksColumn,
						ColumnType = env.DbEnvironment.TextTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = env.DbTableConfig.CreateTimestampColumn,
						ColumnType = env.DbEnvironment.TimestampTypeName,
						IsNullable= false,
						DefaultValue = env.DbEnvironment.TimestampDefaultValue,
					},
				},
				Indexes = new()
				{
					new DbIndexDefinition()
					{
						Identifers = new() { procId.Identifer }
					}
				}
			},
			InterlinkProcessIdColumn = procId.ColumnName,
			InterlinkDestinationIdColumn = DbSequence.ColumnName,
			RootIdColumn = rootColumn,
			OriginIdColumn = originColumn,
			RemarksColumn = env.DbTableConfig.RemarksColumn,
		};

		return t;
	}

	public ReverseRequestTable GetReverseRequestTable(SystemEnvironment env)
	{
		var tablename = string.Format(env.DbTableConfig.ReverseRequestTableNameFormat, DbTable.TableName);
		var idcolumn = string.Format(env.DbTableConfig.RequestIdColumnFormat, tablename);

		var t = new ReverseRequestTable()
		{
			Definition = new()
			{
				SchemaName = env.DbTableConfig.ControlTableSchemaName,
				TableName = tablename,
				ColumnContainers = new()
				{
					new DbColumnDefinition()
					{
						ColumnName = idcolumn,
						ColumnType = env.DbEnvironment.AutoNumberTypeName,
						IsNullable = false,
						IsPrimaryKey = true,
						IsAutoNumber = true,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbSequence.ColumnName,
						ColumnType = env.DbEnvironment.NumericTypeName,
						IsNullable = false,
					},
					new DbColumnDefinition()
					{
						ColumnName = env.DbTableConfig.CreateTimestampColumn,
						ColumnType = env.DbEnvironment.TimestampTypeName,
						IsNullable = false,
						DefaultValue = env.DbEnvironment.TimestampDefaultValue,
					},
				}
			},
			RequestIdColumn = idcolumn,
			DestinationIdColumn = DbSequence.ColumnName,
		};

		return t;
	}

	public SelectQuery ToSelectQuery()
	{
		var sq = new SelectQuery();
		sq.AddComment("destination");
		var (f, d) = sq.From(DbTable.TableFullName).As("d");
		DbTable.ColumnNames.ForEach(x => sq.Select(d, x));
		return sq;
	}

	public InsertQuery CreateInsertQueryFrom(MaterializeResult datasourceMaterial)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(datasourceMaterial.SelectQuery).As("d");

		sq.Select(d);

		// Exclude from selection if it does not exist in the destination column
		sq.SelectClause!.FilterInColumns(DbTable.ColumnNames);

		return sq.ToInsertQuery(DbTable.TableFullName);
	}
}
