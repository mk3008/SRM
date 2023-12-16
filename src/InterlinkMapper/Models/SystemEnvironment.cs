using RedOrb;

namespace InterlinkMapper.Models;

public class SystemEnvironment
{
	public IDbConnetionSetting DbConnetionConfig { get; set; } = null!;

	public DbTableConfig DbTableConfig { get; set; } = new();

	public DbEnvironment DbEnvironment { get; set; } = new();

	public InterlinkTransactionTable GetInterlinkTansactionTable()
	{
		var t = new InterlinkTransactionTable()
		{
			Definition = new()
			{
				SchemaName = DbTableConfig.ControlTableSchemaName,
				TableName = DbTableConfig.InterlinkTransactionTableName,
				ColumnDefinitions = new()
				{
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.InterlinkTransactionIdColumn,
						ColumnType = DbEnvironment.AutoNumberTypeName,
						IsNullable = false,
						IsPrimaryKey = true,
						IsAutoNumber = true,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.InterlinkDatasourceIdColumn,
						ColumnType = DbEnvironment.NumericTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.InterlinkDestinationIdColumn,
						ColumnType = DbEnvironment.NumericTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.ActionNameColumn,
						ColumnType = DbEnvironment.TextTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.ArgumentColumn,
						ColumnType = DbEnvironment.TextTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.TimestampColumn,
						ColumnType = DbEnvironment.TimestampTypeName,
						IsNullable= false,
						DefaultValue = DbEnvironment.TimestampDefaultValue,
					},
				}
			},
			InterlinkTransactionIdColumn = DbTableConfig.InterlinkTransactionIdColumn,
			InterlinkDatasourceIdColumn = DbTableConfig.InterlinkDatasourceIdColumn,
			InterlinkDestinationIdColumn = DbTableConfig.InterlinkDestinationIdColumn,
			ActionNameColumn = DbTableConfig.ActionNameColumn,
			ArgumentColumn = DbTableConfig.ArgumentColumn,
		};
		return t;
	}

	public InterlinkProcessTable GetInterlinkProcessTable()
	{
		var t = new InterlinkProcessTable()
		{
			Definition = new()
			{
				SchemaName = DbTableConfig.ControlTableSchemaName,
				TableName = DbTableConfig.InterlinkProcessTableName,
				ColumnDefinitions = new()
				{
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.InterlinkProcessIdColumn,
						ColumnType = DbEnvironment.AutoNumberTypeName,
						IsNullable= false,
						IsPrimaryKey = true,
						IsAutoNumber = true,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.InterlinkTransactionIdColumn,
						ColumnType = DbEnvironment.NumericTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.InterlinkDatasourceIdColumn,
						ColumnType = DbEnvironment.NumericTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.InterlinkDestinationIdColumn,
						ColumnType = DbEnvironment.NumericTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.ActionNameColumn,
						ColumnType = DbEnvironment.TextTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.InsertCountColumn,
						ColumnType = DbEnvironment.TextTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.KeyMapTableNameColumn,
						ColumnType = DbEnvironment.TextTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.KeyRelationTableNameColumn,
						ColumnType = DbEnvironment.TextTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.TimestampColumn,
						ColumnType = DbEnvironment.TimestampTypeName,
						IsNullable= false,
						DefaultValue = DbEnvironment.TimestampDefaultValue,
					},
				}
			},
			InterlinkProcessIdColumn = DbTableConfig.InterlinkProcessIdColumn,
			InterlinkTransactionIdColumn = DbTableConfig.InterlinkTransactionIdColumn,
			InterlinkDatasourceIdColumn = DbTableConfig.InterlinkDatasourceIdColumn,
			InterlinkDestinationIdColumn = DbTableConfig.InterlinkDestinationIdColumn,
			ActionNameColumn = DbTableConfig.ActionNameColumn,
			InsertCountColumn = DbTableConfig.InsertCountColumn,
			KeyMapTableNameColumn = DbTableConfig.KeyMapTableNameColumn,
			KeyRelationTableNameColumn = DbTableConfig.KeyRelationTableNameColumn,
		};
		return t;
	}

	public InterlinkRelationTable GetInterlinkRelationTable(InterlinkDestination d)
	{
		var rootColumn = string.Format(DbTableConfig.RootIdColumnFormat, d.Sequence.Column);
		var originColumn = string.Format(DbTableConfig.OriginIdColumnFormat, d.Sequence.Column);

		var t = new InterlinkRelationTable()
		{
			Definition = new()
			{
				SchemaName = DbTableConfig.ControlTableSchemaName,
				TableName = string.Format(DbTableConfig.RelationTableNameFormat, d.Table.TableName),
				ColumnDefinitions = new()
				{
					new DbColumnDefinition()
					{
						ColumnName = d.Sequence.Column,
						ColumnType = DbEnvironment.NumericTypeName,
						IsNullable= false,
						IsPrimaryKey = true,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.InterlinkProcessIdColumn,
						ColumnType = DbEnvironment.NumericTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = rootColumn,
						ColumnType = DbEnvironment.NumericTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = originColumn,
						ColumnType = DbEnvironment.NumericTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.RemarksColumn,
						ColumnType = DbEnvironment.TextTypeName,
						IsNullable= false,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.TimestampColumn,
						ColumnType = DbEnvironment.TimestampTypeName,
						IsNullable= false,
						DefaultValue = DbEnvironment.TimestampDefaultValue,
					},
				},
				Indexes = new()
				{
					new DbIndexDefinition()
					{
						Identifers = new() { DbTableConfig.InterlinkProcessIdColumn }
					}
				}
			},
			InterlinkProcessIdColumn = DbTableConfig.InterlinkProcessIdColumn,
			InterlinkDestinationIdColumn = d.Sequence.Column,
			RootIdColumn = rootColumn,
			OriginIdColumn = originColumn,
			RemarksColumn = DbTableConfig.RemarksColumn,
		};
		return t;
	}

	public KeymapTable GetKeyMapTable(InterlinkDatasource d)
	{
		var columndefs = new List<DbColumnDefinition>();

		d.KeyColumns.ForEach(x =>
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
			ColumnName = d.Destination.Sequence.Column,
			ColumnType = DbEnvironment.NumericTypeName,
			IsNullable = true,
			IsUniqueKey = true,
			Comment = @"destination sequence is nullable.
If you want to stop the transfer intentionally, please register the destination sequence as NULL."
		});

		columndefs.Add(new DbColumnDefinition()
		{
			ColumnName = DbTableConfig.TimestampColumn,
			ColumnType = DbEnvironment.TimestampTypeName,
			IsNullable = false,
			DefaultValue = DbEnvironment.TimestampDefaultValue,
		});

		var t = new KeymapTable()
		{
			Definition = new()
			{
				SchemaName = DbTableConfig.ControlTableSchemaName,
				TableName = string.Format(DbTableConfig.KeyMapTableNameFormat, d.Destination.Table.TableName, d.KeyName),
				ColumnDefinitions = columndefs,
				Indexes = new()
				{
					new DbIndexDefinition()
					{
						Identifers = { d.Destination.Sequence.Column },
						IsUnique= true,
					}
				}
			},
			DestinationIdColumn = d.Destination.Sequence.Column,
			DatasourceKeyColumns = d.KeyColumns.Select(x => x.ColumnName).ToList(),
		};
		return t;
	}

	public KeyRelationTable GetKeyRelationTable(InterlinkDatasource d)
	{
		var columndefs = new List<DbColumnDefinition>
		{
			new DbColumnDefinition()
			{
				ColumnName = d.Destination.Sequence.Column,
				ColumnType = DbEnvironment.NumericTypeName,
				IsNullable = false,
				IsPrimaryKey = true,
			}
		};
		d.KeyColumns.ForEach(x =>
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
			ColumnName = DbTableConfig.RemarksColumn,
			ColumnType = DbEnvironment.TextTypeName,
			IsNullable = true,
			IsPrimaryKey = false,
		});
		columndefs.Add(new DbColumnDefinition()
		{
			ColumnName = DbTableConfig.TimestampColumn,
			ColumnType = DbEnvironment.TimestampTypeName,
			IsNullable = false,
			DefaultValue = DbEnvironment.TimestampDefaultValue,
		});
		var t = new KeyRelationTable()
		{
			Definition = new()
			{
				SchemaName = DbTableConfig.ControlTableSchemaName,
				TableName = string.Format(DbTableConfig.KeyRelationTableNameFormat, d.Destination.Table.TableName, d.KeyName),
				ColumnDefinitions = columndefs,
				Indexes = new()
				{
					new DbIndexDefinition()
					{
						Identifers = d.KeyColumns.Select(x => x.ColumnName).ToList(),
					}
				}
			},
			DestinationIdColumn = d.Destination.Sequence.Column,
			DatasourceKeyColumns = d.KeyColumns.Select(x => x.ColumnName).ToList(),
			RemarksColumn = DbTableConfig.RemarksColumn
		};
		return t;
	}

	public ReverseRequestTable GetReverseRequestTable(InterlinkDestination d)
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
					new DbColumnDefinition()
					{
						ColumnName = idcolumn,
						ColumnType = DbEnvironment.AutoNumberTypeName,
						IsNullable = false,
						IsPrimaryKey = true,
						IsAutoNumber = true,
					},
					new DbColumnDefinition()
					{
						ColumnName = d.Sequence.Column,
						ColumnType = DbEnvironment.NumericTypeName,
						IsNullable = false,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.RemarksColumn,
						ColumnType = DbEnvironment.TextTypeName,
						IsNullable = false,
						IsUniqueKey = false,
						DefaultValue = string.Empty,
					},
					new DbColumnDefinition()
					{
						ColumnName = DbTableConfig.TimestampColumn,
						ColumnType = DbEnvironment.TimestampTypeName,
						IsNullable = false,
						DefaultValue = DbEnvironment.TimestampDefaultValue,
					},
				}
			},
			RequestIdColumn = idcolumn,
			DestinationIdColumn = d.Sequence.Column,
			RemarksColumn = DbTableConfig.RemarksColumn,
		};
		return t;
	}

	public ValidationRequestTable GetValidationRequestTable(InterlinkDatasource d)
	{
		var tablename = string.Format(DbTableConfig.ValidateRequestTableNameFormat, d.Destination.Table.TableName, d.KeyName);
		var idcolumn = string.Format(DbTableConfig.RequestIdColumnFormat, tablename);

		var columndefs = new List<DbColumnDefinition>
		{
			new DbColumnDefinition()
			{
				ColumnName = idcolumn,
				ColumnType = DbEnvironment.AutoNumberTypeName,
				IsNullable = false,
				IsPrimaryKey = true,
				IsAutoNumber = true,
			}
		};
		d.KeyColumns.ForEach(x =>
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
			ColumnName = DbTableConfig.TimestampColumn,
			ColumnType = DbEnvironment.TimestampTypeName,
			IsNullable = false,
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

	public InsertRequestTable GetInsertRequestTable(InterlinkDatasource d)
	{
		var tablename = string.Format(DbTableConfig.InsertRequestTableNameFormat, d.Destination.Table.TableName, d.KeyName);
		var idcolumn = string.Format(DbTableConfig.RequestIdColumnFormat, tablename);

		var columndefs = new List<DbColumnDefinition>
		{
			new DbColumnDefinition()
			{
				ColumnName = idcolumn,
				ColumnType = DbEnvironment.AutoNumberTypeName,
				IsNullable = false,
				IsPrimaryKey = true,
				IsAutoNumber = true,
			}
		};
		d.KeyColumns.ForEach(x =>
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
			ColumnName = DbTableConfig.TimestampColumn,
			ColumnType = DbEnvironment.TimestampTypeName,
			IsNullable = false,
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

	public InsertQuery CreateTransactionInsertQuery(InterlinkTransactionRow row)
	{
		var table = GetInterlinkTansactionTable();

		//select :destination_name, :datasoruce_name
		var sq = new SelectQuery();
		sq.Select(DbEnvironment, table.InterlinkDestinationIdColumn, row.InterlinkDestinationId);
		sq.Select(DbEnvironment, table.InterlinkDatasourceIdColumn, row.InterlinkDatasourceId);
		sq.Select(DbEnvironment, table.ArgumentColumn, row.Argument);

		//insert into transaction_table returning transaction_id
		var iq = sq.ToInsertQuery(table.Definition.TableFullName);
		iq.Returning(table.InterlinkTransactionIdColumn);

		return iq;
	}
}
