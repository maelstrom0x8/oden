package io.maelstrom.oden;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardOpenOption.*;

class Bitcask implements Closeable
{
	private static final String tombstone = "0xDEAD";
	private static final KeyDir KEY_DIR = new KeyDir();
	private final BitcaskHandle handle;
	private FileLock lock;

	private Bitcask(BitcaskHandle handle)
	{
		this.handle = handle;
	}

	public static Bitcask Create(Path dataDirectory, int maxFileSize) throws IOException
	{
		BitcaskHandle handle = new BitcaskHandle(dataDirectory, maxFileSize);
		Bitcask bitcask = new Bitcask(handle);
		bitcask.Lock();
		return bitcask;
	}

	public static void Put(BitcaskHandle handle, String key, String value)
	{
		BitcaskFile file;
		try
		{
			file = BitcaskFile.Open(handle.CurrentActiveFile().toString(), WRITE, SYNC, APPEND);
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}
		var pos = handle.Write(file, key, value);
		var entry = MakeEntry(handle.CurrentActiveFile().getFileName().toString(), value.length(), pos, System.currentTimeMillis());
		KEY_DIR.Put(key, entry);
	}

	public static void Delete(Handle handle, String key)
	{
		var entry = KEY_DIR.Get(key);
		if (entry != null)
		{
			try
			{
				BitcaskFile file = BitcaskFile.Open(entry.fileId, WRITE);
				((BitcaskHandle) handle).Write(file, key, tombstone);
				KEY_DIR.Delete(key);
			} catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}
	}

	public static String Get(Handle handle, String key)
	{
		KeyDir.Entry entry = KEY_DIR.Get(key);
		try
		{
			Path path = ((BitcaskHandle) handle).DataDirectory().resolve(entry.fileId);
			var file = BitcaskFile.Open(path.toString(), READ);
			byte[] value = ((BitcaskHandle) handle).Read(file, entry.valuesz, entry.valuepos);
			return new String(value);
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}

	}

	private static KeyDir.Entry MakeEntry(String fileId, int valsz, long valpos, long tstamp)
	{
		return new KeyDir.Entry(fileId, valsz, valpos, tstamp);
	}

	public void Lock()
	{
		this.lock = BitcaskLocks.GetLock(handle.DataDirectory());
	}

	public BitcaskHandle GetHandle()
	{
		return handle;
	}

	@Override
	public void close() throws IOException
	{
		lock.release();
		handle.Close();
	}

	private static class KeyDir
	{

		private final Map<String, Entry> table = new ConcurrentHashMap<>();

		public void Put(String key, Entry entry)
		{
			table.put(key, entry);
		}

		public Entry Get(String key)
		{
			return table.get(key);
		}

		public void Delete(String key)
		{
			table.remove(key);
		}

		public static class Entry
		{
			String fileId;
			int valuesz;
			long valuepos;
			long timestamp;

			public Entry(String fileId, int valuesz, long valuepos, long timestamp)
			{
				this.fileId = fileId;
				this.valuesz = valuesz;
				this.valuepos = valuepos;
				this.timestamp = timestamp;
			}

		}

	}

}
