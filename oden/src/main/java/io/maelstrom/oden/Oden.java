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
			this.bitcask = Bitcask.create(this.options.dataDirectory, this.options.maxFileSize);
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public void Put(String key, String value)
	{
		Bitcask.Put(bitcask.GetHandle(), key, value);
	}

	public String Get(String key)
	{
		return Bitcask.Get(Handle(), key);
	}

	public void Delete(String key)
	{
		Bitcask.Delete(Handle(), key);
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
