package io.maelstrom.oden;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardOpenOption.*;

class Bitcask implements Closeable
{
	private static final String tombstone = "0xDEAD";
	private static final KeyDir KEY_DIR = new KeyDir();
	private final Path dataDirectory;
	private final long maxFileSize;
	private BitcaskHandle handle = null;
	private FileLock lock;
	private BitcaskFile current;
	private Set<String> dataFiles = new HashSet<>();

	public Bitcask(Path dataDirectory, long maxFileSize) throws IOException
	{
		this.maxFileSize = maxFileSize;
		this.dataDirectory = dataDirectory;

		Initialize();
	}

	public static Bitcask Create(Path dataDirectory, long maxFileSize) throws IOException
	{
		Files.createDirectories(dataDirectory);
		return new Bitcask(dataDirectory, maxFileSize);
	}

	private void Initialize() throws IOException
	{
		Lock();
		if (!ReadActiveFile())
		{
			int id_ = new Random().nextInt(1000);

			Path path = dataDirectory.resolve("current-" + id_ + ".odn");
			handle = BitcaskHandle.Open(path.toString(), CREATE, WRITE, APPEND, DSYNC);
		}

		LoadDataFiles();
	}

	private void LoadDataFiles()
	{
		try
		{
			Files.walk(dataDirectory)
				.map(p -> p.getFileName().toString())
				.filter(e -> e.endsWith(".odn"))
				.forEach(dataFiles::add);
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	private boolean ReadActiveFile()
	{
		try
		{
			var dir = dataDirectory.resolve("current");

			if (!Files.exists(dir))
			{
				return false;
			}

			String activeFile = Files.readString(dir);
			if (activeFile.isBlank() || !Files.exists(Path.of(activeFile)))
			{
				return false;
			}

			current = BitcaskFile.Open(dataDirectory.resolve(activeFile).toString(), WRITE, APPEND, DSYNC);
			return true;

		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	private void WriteActiveFile() throws IOException
	{
		Files.writeString(dataDirectory.resolve("current"), handle.file.FileName());
	}

	public void Put(String key, String value)
	{
		var pos = handle.Write(key, value);
		var entry = MakeEntry(handle.file.FileName(), value.length(), pos, System.currentTimeMillis());
		KEY_DIR.Put(key, entry);
	}

	public void Delete(String key)
	{
		KEY_DIR.Delete(key);
		handle.Write(key, tombstone);
	}

	public String Get(String key)
	{
		KeyDir.Entry entry = KEY_DIR.Get(key);
		if (entry == null)
		{
			return null;
		}
		try
		{
			BitcaskHandle handle_;
			if (handle.file.FileName().equals(entry.fileId))
			{
				handle_ = BitcaskHandle.Open(handle.file.GetPath().toString(), READ);
			} else
			{
				handle_ = BitcaskHandle.Open(dataDirectory.resolve(entry.fileId).toString(), READ);
			}

			byte[] value = handle_.Read(entry.valuesz, entry.valuepos);
			handle_.Close();
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
		this.lock = BitcaskLocks.GetLock(dataDirectory);
	}

	public BitcaskHandle GetHandle()
	{
		return handle;
	}

	@Override
	public void close() throws IOException
	{
		WriteActiveFile();
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
