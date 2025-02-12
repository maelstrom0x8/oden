package io.maelstrom.oden;


import java.nio.file.Path;

public class OdenOptions
{

	final Path dataDirectory;
	final long maxFileSize;

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
		var sz = 3 * 1073741824;

		return new OdenOptions(dir, sz);
	}

	public Path GetDataDirectory()
	{
		return dataDirectory;
	}

	public long GetMaxFileSize()
	{
		return maxFileSize;
	}
}
