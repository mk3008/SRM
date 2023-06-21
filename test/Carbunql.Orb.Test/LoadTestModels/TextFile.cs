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
				new () {Identifer = nameof(TextFile.TextFileId), ColumnName = "text_file_id", TypeName= "serial8", IsPrimaryKey= true, IsAutoNumber = true},
				new () {Identifer = nameof(TextFile.TextFileName), ColumnName = "text_file_name", TypeName= "text"},
				new () {Identifer = nameof(TextFile.TextFolder), ColumnName = "text_folder_id", TypeName = "int8", RelationType = typeof(TextFolder)},
			},
			Indexes =
			{
				new () {Identifers = new() { nameof(TextFile.TextFolder), nameof(TextFile.TextFileName)}, IsUnique = true }
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
				new () {Identifer = nameof(TextFolder.TextFolderId), ColumnName = "text_folder_id", TypeName= "serial8", IsPrimaryKey= true, IsAutoNumber = true},
				new () {Identifer = nameof(TextFolder.TextFolderName), ColumnName = "text_folder_name", TypeName= "text"},
			},
			Indexes =
			{
				new () {Identifers = new() {nameof(TextFolder.TextFolderName)}, IsUnique = true }
			}
		};
	}
}