using RedOrb;
using RedOrb.Attributes;

namespace InterlinkMapper.Models;

[DbIndex(isUnique: true, nameof(KeyName))]
[DbTable]
public class InterlinkDatasource
{
	[DbColumn("numeric", IsAutoNumber = true, IsPrimaryKey = true)]
	public long InterlinkDatasourceId { get; set; }

	[DbColumn("text")]
	public required string DatasourceName { get; set; }

	[DbColumn("text")]
	public string Description { get; set; } = string.Empty;

	[DbParentRelationColumn("numeric", nameof(InterlinkDestination.InterlinkDestinationId))]
	public required InterlinkDestination Destination { get; set; }

	[DbColumn("text")]
	public required string Query { get; set; }

	[DbColumn("text", IsUniqueKey = true)]
	public required string KeyName { get; set; }

	[DbColumn("text")]
	public required List<KeyColumn> KeyColumns { get; set; }

	[DbColumn("timestamp", SpecialColumn = SpecialColumn.CreateTimestamp)]
	public DateTime CreatedAt { get; set; }

	[DbColumn("timestamp", SpecialColumn = SpecialColumn.UpdateTimestamp)]
	public DateTime UpdatedAt { get; set; }

	[DbColumn("numeric", SpecialColumn = SpecialColumn.VersionNumber)]
	public long LockVersion { get; set; }

	public InsertRequestTable GetInsertRequestTable(SystemEnvironment env)
	{
		var tablename = string.Format(env.DbTableConfig.InsertRequestTableNameFormat, Destination.DbTable.TableName, KeyName);
		var idcolumn = string.Format(env.DbTableConfig.RequestIdColumnFormat, tablename);

		var columndefs = new List<IDbColumnContainer>
		{
			new DbColumnDefinition()
			{
				ColumnName = idcolumn,
				ColumnType = env.DbEnvironment.AutoNumberTypeName,
				IsNullable = false,
				IsPrimaryKey = true,
				IsAutoNumber = true,
			}
		};

		KeyColumns.ForEach(x =>
		{
			columndefs.Add(new DbColumnDefinition()
			{
				ColumnName = x.ColumnName,
				ColumnType = x.TypeName,
				IsNullable = false,
			});
		});

		columndefs.Add(new DbColumnDefinition()
		{
			ColumnName = env.DbTableConfig.CreateTimestampColumn,
			ColumnType = env.DbEnvironment.TimestampTypeName,
			IsNullable = false,
			DefaultValue = env.DbEnvironment.TimestampDefaultValue,
		});

		var t = new InsertRequestTable()
		{
			Definition = new()
			{
				SchemaName = env.DbTableConfig.ControlTableSchemaName,
				TableName = tablename,
				ColumnContainers = columndefs
			},
			RequestIdColumn = idcolumn,
			DatasourceKeyColumns = KeyColumns.Select(x => x.ColumnName).ToList(),
		};
		return t;
	}

	public ValidationRequestTable GetValidationRequestTable(SystemEnvironment env)
	{
		var tablename = string.Format(env.DbTableConfig.ValidateRequestTableNameFormat, Destination.DbTable.TableName, KeyName);
		var idcolumn = string.Format(env.DbTableConfig.RequestIdColumnFormat, tablename);

		var columndefs = new List<IDbColumnContainer>
		{
			new DbColumnDefinition()
			{
				ColumnName = idcolumn,
				ColumnType = env.DbEnvironment.AutoNumberTypeName,
				IsNullable = false,
				IsPrimaryKey = true,
				IsAutoNumber = true,
			}
		};

		KeyColumns.ForEach(x =>
		{
			columndefs.Add(new DbColumnDefinition()
			{
				ColumnName = x.ColumnName,
				ColumnType = x.TypeName,
				IsNullable = false,
			});
		});

		columndefs.Add(new DbColumnDefinition()
		{
			ColumnName = env.DbTableConfig.CreateTimestampColumn,
			ColumnType = env.DbEnvironment.TimestampTypeName,
			IsNullable = false,
			DefaultValue = env.DbEnvironment.TimestampDefaultValue,
		});

		var t = new ValidationRequestTable()
		{
			Definition = new()
			{
				SchemaName = env.DbTableConfig.ControlTableSchemaName,
				TableName = tablename,
				ColumnContainers = columndefs
			},
			RequestIdColumn = idcolumn,
			DatasourceKeyColumns = KeyColumns.Select(x => x.ColumnName).ToList(),
		};
		return t;
	}

	public KeymapTable GetKeyMapTable(SystemEnvironment env)
	{
		var columndefs = new List<IDbColumnContainer>();

		KeyColumns.ForEach(x =>
		{
			columndefs.Add(new DbColumnDefinition()
			{
				ColumnName = x.ColumnName,
				ColumnType = x.TypeName,
				IsNullable = false,
				IsPrimaryKey = true,
			});
		});

		columndefs.Add(new DbColumnDefinition()
		{
			Identifer = Destination.DbSequence.ColumnName,
			ColumnName = Destination.DbSequence.ColumnName,
			ColumnType = env.DbEnvironment.NumericTypeName,
			IsNullable = true,
			IsUniqueKey = true,
			Comment = @"destination sequence is nullable.
If you want to stop the transfer intentionally, please register the destination sequence as NULL."
		});

		columndefs.Add(new DbColumnDefinition()
		{
			ColumnName = env.DbTableConfig.CreateTimestampColumn,
			ColumnType = env.DbEnvironment.TimestampTypeName,
			IsNullable = false,
			DefaultValue = env.DbEnvironment.TimestampDefaultValue,
		});

		var t = new KeymapTable()
		{
			Definition = new()
			{
				SchemaName = env.DbTableConfig.ControlTableSchemaName,
				TableName = string.Format(env.DbTableConfig.KeyMapTableNameFormat, Destination.DbTable.TableName, KeyName),
				ColumnContainers = columndefs,
				Indexes = new()
				{
					new DbIndexDefinition()
					{
						Identifers = { Destination.DbSequence.ColumnName },
						IsUnique= true,
					}
				}
			},
			DestinationIdColumn = Destination.DbSequence.ColumnName,
			DatasourceKeyColumns = KeyColumns.Select(x => x.ColumnName).ToList(),
		};
		return t;
	}

	public KeyRelationTable GetKeyRelationTable(SystemEnvironment env)
	{
		var columndefs = new List<IDbColumnContainer>
		{
			new DbColumnDefinition()
			{
				ColumnName = Destination.DbSequence.ColumnName,
				ColumnType = env.DbEnvironment.NumericTypeName,
				IsNullable = false,
				IsPrimaryKey = true,
			}
		};

		KeyColumns.ForEach(x =>
		{
			columndefs.Add(new DbColumnDefinition()
			{
				Identifer = x.ColumnName,
				ColumnName = x.ColumnName,
				ColumnType = x.TypeName,
				IsNullable = false,
			});
		});

		columndefs.Add(new DbColumnDefinition()
		{
			ColumnName = env.DbTableConfig.RemarksColumn,
			ColumnType = env.DbEnvironment.TextTypeName,
			IsNullable = true,
			IsPrimaryKey = false,
		});

		columndefs.Add(new DbColumnDefinition()
		{
			ColumnName = env.DbTableConfig.CreateTimestampColumn,
			ColumnType = env.DbEnvironment.TimestampTypeName,
			IsNullable = false,
			DefaultValue = env.DbEnvironment.TimestampDefaultValue,
		});

		var t = new KeyRelationTable()
		{
			Definition = new()
			{
				SchemaName = env.DbTableConfig.ControlTableSchemaName,
				TableName = string.Format(env.DbTableConfig.KeyRelationTableNameFormat, Destination.DbTable.TableName, KeyName),
				ColumnContainers = columndefs,
				Indexes = new()
				{
					new DbIndexDefinition()
					{
						Identifers = KeyColumns.Select(x => x.ColumnName).ToList(),
					}
				}
			},
			DestinationIdColumn = Destination.DbSequence.ColumnName,
			DatasourceKeyColumns = KeyColumns.Select(x => x.ColumnName).ToList(),
			RemarksColumn = env.DbTableConfig.RemarksColumn
		};

		return t;
	}

	public SelectQuery ToSelectQuery()
	{
		var sq = new SelectQuery(Query);
		sq.AddComment("raw data source");
		return sq;
	}
}
