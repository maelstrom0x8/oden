package io.maelstrom.oden;


import java.nio.file.Path;

public class OdenOptions
{

	final Path dataDirectory;
	final int maxFileSize;

	public OdenOptions(String dataDirectory, int maxFileSize)
	{
		if (!dataDirectory.startsWith("/"))
		{
			Path cwd = Path.of(System.getProperty("user.dir"));
			this.dataDirectory = cwd.resolve(dataDirectory).normalize().toAbsolutePath();
		} else
		{
			this.dataDirectory = Path.of(dataDirectory);
		}

		this.maxFileSize = maxFileSize;
	}

	public static OdenOptions Default()
	{
		String homeDir = System.getProperty("user.home");
		var dir = homeDir + "/.oden/data";
		var sz = 1024 * 1024 * 1024;

		return new OdenOptions(dir, sz);
	}

	public Path GetDataDirectory()
	{
		return dataDirectory;
	}

	public int GetMaxFileSize()
	{
		return maxFileSize;
	}
}
