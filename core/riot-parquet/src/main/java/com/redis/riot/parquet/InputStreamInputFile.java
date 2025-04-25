package com.redis.riot.parquet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * This class is needed to convert the InputStream into a format read by
 * ParquetFileReader
 */
public class InputStreamInputFile implements InputFile {

	private final InputStream stream;
	private long length;

	public InputStreamInputFile(InputStream stream) {
		this.stream = stream;
	}

	@Override
	public long getLength() {
		return length;
	}

	@Override
	public SeekableInputStream newStream() throws IOException {
		byte[] data = stream.readAllBytes();
		length = data.length;
		SeekableByteArrayInputStream bais = new SeekableByteArrayInputStream(data);

		return HadoopStreams.wrap(new FSDataInputStream(bais));
	}

	static class SeekableByteArrayInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable {

		public SeekableByteArrayInputStream(byte[] buf) {
			super(buf);
		}

		@Override
		public long getPos() {
			return pos;
		}

		@Override
		public void seek(long pos) throws IOException {
			if (mark != 0)
				throw new IllegalStateException();

			reset();
			long skipped = skip(pos);

			if (skipped != pos)
				throw new IOException();
		}

		@Override
		public boolean seekToNewSource(long targetPos) {
			return false;
		}

		@Override
		public int read(long position, byte[] buffer, int offset, int length) throws IOException {

			if (position >= buf.length || position + length > buf.length || length > buffer.length)
				throw new IllegalArgumentException();

			System.arraycopy(buf, (int) position, buffer, offset, length);
			return length;
		}

		@Override
		public void readFully(long position, byte[] buffer) throws IOException {
			read(position, buffer, 0, buffer.length);
		}

		@Override
		public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
			read(position, buffer, offset, length);
		}
	}
}