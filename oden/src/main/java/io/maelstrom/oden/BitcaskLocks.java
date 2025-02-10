package io.maelstrom.oden;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

class BitcaskLocks
{
	public static FileLock GetLock(Path path)
	{
		try
		{
			var lockFile = path.resolve(Path.of("oden.lck"));
			if (Files.exists(lockFile))
			{
				throw new IllegalStateException("unable to lock directory");
			}
			FileChannel channel = FileChannel.open(lockFile, CREATE, WRITE);
			return new BitcaskLock(lockFile, channel, 0, Long.MAX_VALUE, false);
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}

	}

	private static class BitcaskLock extends FileLock
	{

		private final Path lockFile;

		protected BitcaskLock(Path path, FileChannel channel, long position, long size, boolean shared)
		{
			super(channel, position, size, shared);
			this.lockFile = path;
		}


		@Override
		public boolean isValid()
		{
			return Files.exists(lockFile);
		}

		@Override
		public void release() throws IOException
		{
			Files.delete(lockFile);
			channel().close();
		}
	}

}
