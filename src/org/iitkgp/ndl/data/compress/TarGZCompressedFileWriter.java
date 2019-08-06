package org.iitkgp.ndl.data.compress;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;

/**
 * TAR.GZ Compress data writer
 * @author Debasis
 */
public class TarGZCompressedFileWriter implements CompressedDataWriter {
	
	File outLocation;
	String fileName;
	TarArchiveOutputStream compressedOut = null;
	
	/**
	 * Constructor
	 * @param outLocation output location to write data
	 * @param fileName output file name
	 */
	public TarGZCompressedFileWriter(String outLocation, String fileName) {
		this.outLocation = new File(outLocation);
		this.fileName = fileName;
	}
	
	/**
	 * Constructor
	 * @param outLocation output location to write data
	 * @param fileName output file name
	 */
	public TarGZCompressedFileWriter(File outLocation, String fileName) {
		this.outLocation = outLocation;
		this.fileName = fileName;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void init() throws IOException {
		OutputStream outputStream = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(
				new File(createFolder(outLocation), fileName + CompressedFileMode.TARGZ.getMode()))));
		compressedOut = new TarArchiveOutputStream(outputStream);
		// handle exceptions for default settings
		compressedOut.setAddPaxHeadersForNonAsciiNames(true);
		compressedOut.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR);
		compressedOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
	}

	// internal usage
	File createFolder(File location) {
		if(!location.exists()) {
			// make directory
			location.mkdirs();
		}
		return location;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(Map<String, byte[]> contents) throws IOException {
		// compressed-out
		for(String name : contents.keySet()) {
			byte[] bytes = contents.get(name);
			write(name, bytes);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(String name, byte[] bytes) throws IOException {
		TarArchiveEntry newEntry = new TarArchiveEntry(name);
		newEntry.setSize(bytes.length);
		compressedOut.putArchiveEntry(newEntry);
		compressedOut.write(bytes);
		compressedOut.closeArchiveEntry();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(compressedOut);
	}

}