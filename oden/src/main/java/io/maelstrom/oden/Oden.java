package io.maelstrom.oden;

import java.io.IOException;

public class Oden
{

	private final Bitcask bitcask;
	private final OdenOptions options;

	public Oden(OdenOptions options)
	{
		this.options = options == null ? OdenOptions.Default() : options;
		try
		{
			this.bitcask = Bitcask.Create(this.options.dataDirectory, this.options.maxFileSize);
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public void Put(String key, String value)
	{
		bitcask.Put(key, value);
	}

	public String Get(String key)
	{
		return bitcask.Get(key);
	}

	public void Delete(String key)
	{
		bitcask.Delete(key);
	}

	public Handle Handle()
	{
		return bitcask.GetHandle();
	}

	public OdenOptions GetOptions()
	{
		return this.options;
	}

	public void Close()
	{
		try
		{
			bitcask.close();
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

}
