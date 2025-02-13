package io.maelstrom.oden;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;

class BitcaskFile
{
	private final FileChannel channel;
	private final Path path;

	private BitcaskFile(Path path) throws IOException
	{
		this.path = path;
		channel = FileChannel.open(path);
	}

	private BitcaskFile(Path path, OpenOption... options) throws IOException
	{
		this.path = path;
		channel = FileChannel.open(path, options);
	}

	public static BitcaskFile Open(String filename, OpenOption... options) throws IOException
	{
		return new BitcaskFile(Path.of(filename), options);
	}

	public static BitcaskFile Open(String filename) throws IOException
	{
		return new BitcaskFile(Path.of(filename));
	}

	public FileChannel GetChannel()
	{
		return channel;
	}

	public void Close()
	{
		try
		{
			channel.close();
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public long Size()
	{
		try
		{
			return Files.size(path);
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public Path GetPath()
	{
		return path;
	}

	public String FileName()
	{
		return path.getFileName().toString();
	}

	public int Id()
	{

		String[] parts = path.getFileName().toString().split("_");
		return Integer.parseInt(parts[1].split("\\.")[0]);
	}

	@Override
	public String toString()
	{
		return path.toString();
	}
}
