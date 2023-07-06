namespace Carbunql.Orb.Test.LoadTestModels;

public class TextFile
{
	public long? TextFileId { get; set; }

	public string TextFileName { get; set; } = string.Empty;

	public TextFolder TextFolder { get; set; } = new();
}

public class TextFolder
{
	public long? TextFolderId { get; set; }

	public string TextFolderName { get; set; } = string.Empty;

	public TextFolder? ParentTextFolder { get; set; }
}

public static class LoadTestDefinitions
{
	public static DbTableDefinition<TextFile> GetTextFileDefinition()
	{
		return new DbTableDefinition<TextFile>()
		{
			TableName = "text_files",
			ColumnDefinitions =
			{
				new () {Identifer = nameof(TextFile.TextFileId), ColumnName = "text_file_id", ColumnType= "serial8", IsPrimaryKey= true, IsAutoNumber = true},
				new () {Identifer = nameof(TextFile.TextFileName), ColumnName = "text_file_name", ColumnType= "text"},
				new () {ColumnName = "text_folder_id", ColumnType = "int8"},
			},
			//Indexes =
			//{
			//	new () {Identifers = new() { nameof(TextFile.TextFolder), nameof(TextFile.TextFileName)}, IsUnique = true }
			//},
			ParentRelations = {
				new () {Identifer = nameof(TextFile.TextFolder), ColumnNames = { "text_folder_id" } , IdentiferType = typeof(TextFolder)}
			}
		};
	}

	public static DbTableDefinition<TextFolder> GetTextFolderDefinition()
	{
		return new DbTableDefinition<TextFolder>()
		{
			TableName = "text_folders",
			ColumnDefinitions =
			{
				new () {Identifer = nameof(TextFolder.TextFolderId), ColumnName = "text_folder_id", ColumnType= "serial8", IsPrimaryKey= true, IsAutoNumber = true},
				new () {Identifer = nameof(TextFolder.TextFolderName), ColumnName = "text_folder_name", ColumnType= "text"},
				//new () {ColumnName = "parent_text_folder_id", ColumnType = "int8"},
			},
			Indexes =
			{
				new () {Identifers = new() {nameof(TextFolder.TextFolderName)}, IsUnique = true }
			},
			//ParentRelations = {
			//	new () {Identifer = nameof(TextFile.TextFolder), ColumnNames = { "parent_text_folder_id" } , IdentiferType = typeof(TextFolder), IsNullable = true}
			//}
		};
	}
}