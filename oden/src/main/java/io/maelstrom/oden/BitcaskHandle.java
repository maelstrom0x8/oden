package io.maelstrom.oden;


import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

class BitcaskHandle implements Handle
{

	private final Path dataDirectory;
	private final int maxFileSize;
	private Path currentActiveFile;
	private FileOutputStream out;
	private FileChannel channel;
	private ExecutorService executorService;


	public BitcaskHandle(Path dataDirectory, int maxFileSize)
	{
		this.dataDirectory = dataDirectory.normalize();
		this.maxFileSize = maxFileSize;

		try
		{
			if (!Files.exists(dataDirectory))
			{
				Files.createDirectory(dataDirectory);
			}


			if (!ReadActiveFile())
			{
				long fileId = System.currentTimeMillis();
				this.currentActiveFile = dataDirectory.resolve("current-" + fileId);
				Files.createFile(currentActiveFile);
			}

		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public Path DataDirectory()
	{
		return dataDirectory;
	}

	public int MaxFileSize()
	{
		return maxFileSize;
	}

	public Path CurrentActiveFile()
	{
		return currentActiveFile;
	}

	public byte[] Read(BitcaskFile file, int size) throws IOException
	{
		return Read(file, size, 0);
	}

	public byte[] Read(BitcaskFile file, int size, long offset) throws IOException
	{
		FileChannel channel_ = file.GetChannel();
		ByteBuffer buffer = ByteBuffer.allocate(size);
		channel_.read(buffer, offset);
		return buffer.array();
	}

	public long Write(BitcaskFile file, String key, String value)
	{
		var tsz = System.currentTimeMillis();
		var ksz = key.length();
		var valsz = value.length();

		ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + ksz + 4 + valsz);
		buffer.putLong(tsz);
		buffer.putInt(ksz);
		buffer.putInt(valsz);

		buffer.put(key.getBytes());
		buffer.put(value.getBytes());

		FileChannel channel_ = file.GetChannel();
		try
		{
			buffer.flip();
			int written = channel_.write(buffer);
			long position = channel_.position();
			return position - written;
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		} finally
		{
			file.Close();
		}

	}

	public boolean ReadActiveFile()
	{
		try
		{
			var dir = dataDirectory.resolve("current");

			if (Files.exists(dir))
			{
				String activeFile = Files.readString(dir);
				this.currentActiveFile = dataDirectory.resolve(activeFile);
				return true;
			}
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}
		return false;
	}

	public void WriteActiveFile()
	{
		try (FileOutputStream stream = new FileOutputStream(dataDirectory.resolve("current").toFile()))
		{
			stream.write(currentActiveFile.getFileName().toString().getBytes());
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public void Open()
	{
		try
		{
			out = new FileOutputStream(currentActiveFile.toFile(), true);
//			this.channel = FileChannel.open(currentActiveFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public void Close()
	{
		WriteActiveFile();

//		executorService.shutdown();
	}
}
