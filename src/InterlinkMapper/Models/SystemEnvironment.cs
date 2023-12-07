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

	public ReverseTable GetReverseTable(DbDestination d)
	{
		var rootColumn = string.Format(DbTableConfig.RootIdColumnFormat, d.Sequence.Column);
		var originColumn = string.Format(DbTableConfig.OriginIdColumnFormat, d.Sequence.Column);
		var t = new ReverseTable()
		{
			Definition = new()
			{
				SchemaName = DbTableConfig.ControlTableSchemaName,
				TableName = string.Format(DbTableConfig.ReverseTableNameFormat, d.Table.TableName),
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
						ColumnName = rootColumn,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull = false,
						IsUniqueKey = false,
					},
					new ColumnDefinition()
					{
						ColumnName = originColumn,
						TypeName = DbEnvironment.NumericTypeName,
						AllowNull = false,
						IsUniqueKey = true,
					},
					new ColumnDefinition()
					{
						ColumnName = DbTableConfig.RemarksColumn,
						TypeName = DbEnvironment.TextTypeName,
						AllowNull = false,
						IsUniqueKey = false,
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
						Columns = [rootColumn]
					}
				}
			},
			RootIdColumn = rootColumn,
			OriginIdColumn = originColumn,
			ReverseIdColumn = d.Sequence.Column,
			RemarksColumn = DbTableConfig.RemarksColumn,
		};
		return t;
	}

	public ReverseRequestTable GetReverseRequestTable(DbDestination d)
	{
		var tablename = string.Format(DbTableConfig.ReverseRequestTableNameFormat, d.Table.TableName);
		var idcolumn = string.Format(DbTableConfig.RequestIdColumnFormat, tablename);

		var t = new ReverseRequestTable()
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
						ColumnName = DbTableConfig.RemarksColumn,
						TypeName = DbEnvironment.TextTypeName,
						AllowNull = false,
						IsUniqueKey = false,
						DefaultValue = string.Empty,
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
			RemarksColumn = DbTableConfig.RemarksColumn,
		};
		return t;
	}

	public ValidationRequestTable GetValidationRequestTable(DbDatasource d)
	{
		var tablename = string.Format(DbTableConfig.ValidateRequestTableNameFormat, d.Destination.Table.TableName, d.KeyName);
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

		var t = new ValidationRequestTable()
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

	public InsertQuery CreateTransactionInsertQuery(TransactionRow row)
	{
		var table = GetTansactionTable();

		//select :destination_name, :datasoruce_name
		var sq = new SelectQuery();
		sq.Select(DbEnvironment, table.DestinationIdColumn, row.DestinationId);
		sq.Select(DbEnvironment, table.DatasourceIdColumn, row.DatasourceId);
		sq.Select(DbEnvironment, table.ArgumentColumn, row.Argument);

		//insert into transaction_table returning transaction_id
		var iq = sq.ToInsertQuery(table.Definition.TableFullName);
		iq.Returning(table.TransactionIdColumn);

		return iq;
	}

	public InsertQuery CreateProcessInsertQuery(ProcessRow row)
	{
		var table = GetProcessTable();

		//select :transaction_id, :destination_name, :datasoruce_name, :keymap_table, :relationmap_table
		var sq = new SelectQuery();
		sq.Select(DbEnvironment, table.TransactionIdColumn, row.TransactionId);
		sq.Select(DbEnvironment, table.DatasourceIdColumn, row.DatasourceId);
		sq.Select(DbEnvironment, table.DestinationIdColumn, row.DestinationId);
		sq.Select(DbEnvironment, table.KeymapTableNameColumn, row.KeymapTableName);
		sq.Select(DbEnvironment, table.ActionColumn, row.ActionName);
		sq.Select(DbEnvironment, table.InsertCountColumn, row.InsertCount);

		//insert into process_table returning process_id
		var iq = sq.ToInsertQuery(table.Definition.GetTableFullName());
		iq.Returning(table.ProcessIdColumn);

		return iq;
	}
}
