namespace InterlinkMapper.Models;

public class SystemEnvironment
{
	public IDbConnetionSetting DbConnetionConfig { get; set; } = null!;

	public DbTableConfig DbTableConfig { get; set; } = new();

	public DbEnvironment DbEnvironment { get; set; } = new();

	public TransactionTable GetTansactionTable()
	{
		var t = new TransactionTable()
		{
			Definition = new()
			{
				SchemaName = DbTableConfig.ControlTableSchemaName,
				TableName = DbTableConfig.TransactionTableName,
				ColumnDefinitions = new()
				{
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.TransactionIdColumn,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull= false,
						IsPrimaryKey = true,
						IsAutoNumber = true,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.DatasourceIdColumn,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull= false,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.DestinationIdColumn,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull= false,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.ActionNameColumn,
						TypeName = DbEnvironment.TextTypeName,
						AllowNull= false,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.ArgumentColumn,
						TypeName = DbEnvironment.TextTypeName,
						AllowNull= false,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.TimestampColumn,
						TypeName = DbEnvironment.TimestampTypeName,
						AllowNull= false,
						DefaultValue = DbEnvironment.TimestampDefaultValue,
					},
				}
			},
			TransactionIdColumn = DbTableConfig.TransactionIdColumn,
			DatasourceIdColumn = DbTableConfig.DatasourceIdColumn,
			DestinationIdColumn = DbTableConfig.DestinationIdColumn,
			ActionColumn = DbTableConfig.ActionNameColumn,
			ArgumentColumn = DbTableConfig.ArgumentColumn,
		};
		return t;
	}

	public ProcessTable GetProcessTable()
	{
		var t = new ProcessTable()
		{
			Definition = new()
			{
				SchemaName = DbTableConfig.ControlTableSchemaName,
				TableName = DbTableConfig.ProcessTableName,
				ColumnDefinitions = new()
				{
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.ProcessIdColumn,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull= false,
						IsPrimaryKey = true,
						IsAutoNumber = true,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.TransactionIdColumn,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull= false,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.DatasourceIdColumn,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull= false,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.DestinationIdColumn,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull= false,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.ActionNameColumn,
						TypeName = DbEnvironment.TextTypeName,
						AllowNull= false,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.InsertCountColumn,
						TypeName = DbEnvironment.TextTypeName,
						AllowNull= false,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.KeymapTableNameColumn,
						TypeName = DbEnvironment.TextTypeName,
						AllowNull= false,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.TimestampColumn,
						TypeName = DbEnvironment.TimestampTypeName,
						AllowNull= false,
						DefaultValue = DbEnvironment.TimestampDefaultValue,
					},
				}
			},
			ProcessIdColumn = DbTableConfig.ProcessIdColumn,
			TransactionIdColumn = DbTableConfig.TransactionIdColumn,
			DatasourceIdColumn = DbTableConfig.DatasourceIdColumn,
			DestinationIdColumn = DbTableConfig.DestinationIdColumn,
			ActionColumn = DbTableConfig.ActionNameColumn,
			InsertCountColumn = DbTableConfig.InsertCountColumn,
			KeymapTableNameColumn = DbTableConfig.KeymapTableNameColumn
		};
		return t;
	}

	public RelationTable GetRelationTable(DbDestination d)
	{
		var t = new RelationTable()
		{
			Definition = new()
			{
				SchemaName = DbTableConfig.ControlTableSchemaName,
				TableName = string.Format(DbTableConfig.RelationTableNameFormat, d.Table.TableName),
				ColumnDefinitions = new()
				{
					new ColumnDefinition()
					{
						ColumnName = d.Sequence.Column,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull= false,
						IsPrimaryKey = true,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.ProcessIdColumn,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull= false,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.ProcessIdColumn,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull= false,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.TimestampColumn,
						TypeName = DbEnvironment.TimestampTypeName,
						AllowNull= false,
						DefaultValue = DbEnvironment.TimestampDefaultValue,
					},
				},
				Indexes = new()
				{
					new DbIndexDefinition()
					{
						IndexNumber = 1,
						Columns = new() { DbTableConfig.ProcessIdColumn }
					}
				}
			},
			ProcessIdColumn = DbTableConfig.ProcessIdColumn,
			DestinationSequenceColumn = d.Sequence.Column,
		};
		return t;
	}

	public KeymapTable GetKeymapTable(DbDatasource d)
	{
		var columndefs = new List<ColumnDefinition>
		{
			new ColumnDefinition()
			{
				ColumnName = d.Destination.Sequence.Column,
				TypeName = DbEnvironment.NumericTypeName,
				AllowNull = false,
				IsPrimaryKey = true,
			}
		};
		d.KeyColumns.ForEach(x =>
		{
			columndefs.Add(new ColumnDefinition()
			{
				ColumnName = x.ColumnName,
				TypeName = x.TypeName,
				AllowNull = false,
			});
		});
		columndefs.Add(new ColumnDefinition()
		{
			ColumnName = DbTableConfig.TimestampColumn,
			TypeName = DbEnvironment.TimestampTypeName,
			AllowNull = false,
			DefaultValue = DbEnvironment.TimestampDefaultValue,
		});

		var t = new KeymapTable()
		{
			Definition = new()
			{
				SchemaName = DbTableConfig.ControlTableSchemaName,
				TableName = string.Format(DbTableConfig.KeymapTableNameFormat, d.Destination.Table.TableName, d.KeyName),
				ColumnDefinitions = columndefs,
				Indexes = new()
				{
					new DbIndexDefinition()
					{
						IndexNumber = 1,
						Columns = d.KeyColumns.Select(x => x.ColumnName).ToList(),
						IsUnique= true,
					}
				}
			},
			DestinationSequenceColumn = d.Destination.Sequence.Column,
			DatasourceKeyColumns = d.KeyColumns.Select(x => x.ColumnName).ToList(),
		};
		return t;
	}

	public ReversalTable GetReversalTable(DbDestination d)
	{
		if (d.ReversalOption == null) throw new NotSupportedException();

		var reversalColumn = string.Format(DbTableConfig.ReversalIdColumnFormat, d.Sequence.Column);
		var t = new ReversalTable()
		{
			Definition = new()
			{
				SchemaName = DbTableConfig.ControlTableSchemaName,
				TableName = string.Format(DbTableConfig.ReversalTableNameFormat, d.Table.TableName),
				ColumnDefinitions = new()
				{
					new ColumnDefinition()
					{
						ColumnName = d.Sequence.Column,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull = false,
						IsPrimaryKey = true,
					},
					new ColumnDefinition()
					{
						ColumnName = reversalColumn,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull = false,
						IsUniqueKey = true,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.TimestampColumn,
						TypeName = DbEnvironment.TimestampTypeName,
						AllowNull= false,
						DefaultValue = DbEnvironment.TimestampDefaultValue,
					},
				}
			},
			OriginIdColumn = d.Sequence.Column,
			ReversalIdColumn = reversalColumn
		};
		return t;
	}

	public ReversalRequestTable GetReversalRequestTable(DbDestination d)
	{
		if (d.ReversalOption == null) throw new NotSupportedException();

		var tablename = string.Format(DbTableConfig.ReversalRequestTableNameFormat, d.Table.TableName);
		var idcolumn = string.Format(DbTableConfig.RequestIdColumnFormat, tablename);

		var t = new ReversalRequestTable()
		{
			Definition = new()
			{
				SchemaName = DbTableConfig.ControlTableSchemaName,
				TableName = tablename,
				ColumnDefinitions = new()
				{
					new ColumnDefinition()
					{
						ColumnName = idcolumn,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull = false,
						IsPrimaryKey = true,
						IsAutoNumber = true,
					},
					new ColumnDefinition()
					{
						ColumnName = d.Sequence.Column,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull = false,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.TimestampColumn,
						TypeName = DbEnvironment.TimestampTypeName,
						AllowNull= false,
						DefaultValue = DbEnvironment.TimestampDefaultValue,
					},
				}
			},
			RequestIdColumn = idcolumn,
			DestinationSequenceColumn = d.Sequence.Column,
		};
		return t;
	}

	public ValidateRequestTable GetValidateRequestTable(DbDestination d)
	{
		if (d.ReversalOption == null) throw new NotSupportedException();

		var tablename = string.Format(DbTableConfig.ValidateRequestTableNameFormat, d.Table.TableName);
		var idcolumn = string.Format(DbTableConfig.RequestIdColumnFormat, tablename);

		var t = new ValidateRequestTable()
		{
			Definition = new()
			{
				SchemaName = DbTableConfig.ControlTableSchemaName,
				TableName = tablename,
				ColumnDefinitions = new()
				{
					new ColumnDefinition()
					{
						ColumnName = idcolumn,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull = false,
						IsPrimaryKey = true,
						IsAutoNumber = true,
					},
					new ColumnDefinition()
					{
						ColumnName = d.Sequence.Column,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull = false,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.TimestampColumn,
						TypeName = DbEnvironment.TimestampTypeName,
						AllowNull= false,
						DefaultValue = DbEnvironment.TimestampDefaultValue,
					},
				}
			},
			RequestIdColumn = idcolumn,
			DestinationSequenceColumn = d.Sequence.Column,
		};
		return t;
	}

	public InsertRequestTable GetInsertRequestTable(DbDatasource d)
	{
		var tablename = string.Format(DbTableConfig.InsertRequestTableNameFormat, d.Destination.Table.TableName, d.KeyName);
		var idcolumn = string.Format(DbTableConfig.RequestIdColumnFormat, tablename);

		var columndefs = new List<ColumnDefinition>
		{
			new ColumnDefinition()
			{
				ColumnName = idcolumn,
				TypeName = DbEnvironment.NumericTypeName,
				AllowNull = false,
				IsPrimaryKey = true,
				IsAutoNumber = true,
			}
		};
		d.KeyColumns.ForEach(x =>
		{
			columndefs.Add(new ColumnDefinition()
			{
				ColumnName = x.ColumnName,
				TypeName = x.TypeName,
				AllowNull = false,
			});
		});
		columndefs.Add(new ColumnDefinition()
		{
			ColumnName = DbTableConfig.TimestampColumn,
			TypeName = DbEnvironment.TimestampTypeName,
			AllowNull = false,
			DefaultValue = DbEnvironment.TimestampDefaultValue,
		});

		var t = new InsertRequestTable()
		{
			Definition = new()
			{
				SchemaName = DbTableConfig.ControlTableSchemaName,
				TableName = tablename,
				ColumnDefinitions = columndefs
			},
			RequestIdColumn = idcolumn,
			DatasourceKeyColumns = d.KeyColumns.Select(x => x.ColumnName).ToList(),
		};
		return t;
	}
}
