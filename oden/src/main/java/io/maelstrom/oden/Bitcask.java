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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.file.StandardOpenOption.*;
import static java.nio.file.attribute.PosixFilePermission.*;

class Bitcask implements Closeable
{
	private static final String tombstone = "0xDEAD";
	private static final KeyDir KEY_DIR = new KeyDir();
	private final Path dataDirectory;
	private final long maxFileSize;
	private BitcaskHandle handle = null;
	private FileLock lock;
	private BitcaskFile current;
	private final Set<String> dataFiles = new HashSet<>();
	private final ExecutorService executor = Executors.newFixedThreadPool(4);

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

			Path path = dataDirectory.resolve(System.currentTimeMillis() + "_" + id_ + ".active.odn");
			handle = BitcaskHandle.Open(path.toString(), CREATE, WRITE, APPEND, DSYNC);
			current = handle.file;
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

	public Set<String> GetDataFiles()
	{
		return dataFiles;
	}


	private void WriteActiveFile() throws IOException
	{
		Files.writeString(dataDirectory.resolve("current"), handle.file.FileName());
	}

	/**
	 * Rotates the active file by creating a new file and closing the current one.
	 * The new file is then set as the active file.
	 */
	public void RotateActiveFile() throws Exception
	{
		var prev = current;
		var id_ = prev.Id() + 1;
		handle.Close();
		String filename = System.currentTimeMillis() + "_" + id_ + ".active.odn";
		String path = dataDirectory.resolve(filename).toString();
		handle = BitcaskHandle.Open(path, CREATE, WRITE, APPEND, DSYNC);
		current = handle.file;

		WriteActiveFile();
		dataFiles.add(current.FileName());
		Files.setPosixFilePermissions(prev.GetPath(), Set.of(GROUP_READ, OTHERS_READ, OWNER_READ));
		String new_name = prev.FileName().replace("active", "old");
		Files.move(prev.GetPath(), dataDirectory.resolve(new_name));
	}


	/**
	 * Checks if the current active file has reached the maximum file size threshold.
	 * If the threshold is reached, rotates the active file.
	 */
	public void BeginThresholdCheck()
	{
		if (current.Size() < maxFileSize)
		{
			return;
		}

		try
		{
			RotateActiveFile();
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}


	public void Put(String key, String value)
	{

		BeginThresholdCheck();

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
