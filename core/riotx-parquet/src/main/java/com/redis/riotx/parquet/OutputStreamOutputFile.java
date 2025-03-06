package com.redis.riotx.parquet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Syncable;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * This class is needed to convert the OutputStream into a format used by
 * ParquetFileWriter
 */
public class OutputStreamOutputFile implements OutputFile {

	private final OutputStream outputStream;

	public OutputStreamOutputFile(OutputStream outputStream) {
		this.outputStream = outputStream;
	}

	@Override
	public PositionOutputStream create(long blockSizeHint) throws IOException {
		return HadoopStreams.wrap(new FSDataOutputStream(new SyncableOutputStream(outputStream), null));
	}

	@Override
	public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
		return create(blockSizeHint);
	}

	@Override
	public boolean supportsBlockSize() {
		return false;
	}

	@Override
	public long defaultBlockSize() {
		return 0;
	}

	/**
	 * OutputStream implementation that supports position tracking and
	 * synchronization
	 */
	static class SyncableOutputStream extends ByteArrayOutputStream implements Syncable {
		private final OutputStream wrappedStream;
		private long position = 0;

		public SyncableOutputStream(OutputStream outputStream) {
			this.wrappedStream = outputStream;
		}

		@Override
		public void write(int b) {
			super.write(b);
			position++;
		}

		@Override
		public void write(byte[] b) throws IOException {
			super.write(b);
			position += b.length;
		}

		@Override
		public void write(byte[] b, int off, int len) {
			super.write(b, off, len);
			position += len;
		}

		@Override
		public void close() throws IOException {
			flush();
			wrappedStream.close();
			super.close();
		}

		@Override
		public void flush() throws IOException {
			wrappedStream.write(toByteArray());
			wrappedStream.flush();
			reset();
		}

		@Override
		public void hflush() throws IOException {
			flush();
		}

		@Override
		public void hsync() throws IOException {
			flush();
		}

		public long getPos() {
			return position;
		}
	}
}