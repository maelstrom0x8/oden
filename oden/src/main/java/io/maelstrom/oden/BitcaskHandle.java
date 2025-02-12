package io.maelstrom.oden;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

class BitcaskHandle implements Handle
{
	BitcaskFile file;
	FileChannel channel_;

	BitcaskHandle(BitcaskFile file)
	{
		this.file = file;
		this.channel_ = file.GetChannel();
	}

	public static BitcaskHandle Open(String path, StandardOpenOption... options) throws IOException
	{
		BitcaskFile file = BitcaskFile.Open(path, options);
		return new BitcaskHandle(file);
	}

	public byte[] Read(int size) throws IOException
	{
		return Read(size, 0);
	}

	public byte[] Read(int size, long offset) throws IOException
	{
		ByteBuffer buffer = ByteBuffer.allocate(size);
		channel_.position(offset);
		channel_.read(buffer);
		return buffer.array();
	}

	public long Write(String key, String value)
	{
		var tsz = System.currentTimeMillis();
		var ksz = key.length();
		var valsz = value.length();

		ByteBuffer buffer = ByteBuffer.allocate( 8 + 4 + ksz + 4 + valsz + 8);
		buffer.putLong(8, tsz);
		buffer.position(16);
		buffer.putInt(ksz);
		buffer.putInt(valsz);
		buffer.put(key.getBytes());
		buffer.put(value.getBytes());

		int i = buffer.capacity() - 8;
		byte[] bytes = new byte[i];
		buffer.get(8, bytes);

		CRC32 crc32 = new CRC32();
		crc32.update(bytes);
		long crc = crc32.getValue();

		buffer.position(0);
		buffer.putLong(crc);

		try
		{
			channel_.write(buffer);
			long position = channel_.position();
			return position - valsz;
		} catch (IOException e)
		{
			e.printStackTrace(System.err);
		}

		return -1;
	}

	@Override
	public void Open()
	{
		if (!channel_.isOpen())
		{
			this.channel_ = file.GetChannel();
		}
	}

	@Override
	public void Close()
	{
		try
		{
			if (channel_ != null && channel_.isOpen())
			{
				channel_.close();
			}
		} catch (IOException e)
		{
			e.printStackTrace(System.err);
		}
	}


}
