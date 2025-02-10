package io.maelstrom.oden;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;

class BitcaskFile
{
	private final FileChannel channel;

	private BitcaskFile(Path path) throws IOException
	{
		channel = FileChannel.open(path);
	}

	private BitcaskFile(Path path, OpenOption... options) throws IOException
	{
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
}
