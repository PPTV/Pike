package com.pplive.pike.exec.output;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import com.pplive.pike.base.Period;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pplive.pike.Configuration;
import com.pplive.pike.base.ISizeAwareIterable;
import com.pplive.pike.util.Path;

class HDFSOutput implements IPikeOutput {

	public static final Logger log = LoggerFactory.getLogger(HDFSOutput.class);
	private static final String ZIP_CHARTSET_NAME = "GBK";

	private String hdfsHost;
	private String hdfsPath;
	private String localPath;
	private String fieldSeparator;
	private String dataSeparator;
	private boolean compressed;

	@SuppressWarnings("rawtypes")
	private Map conf;

	private OutputSchema outputSchema;

	private String _fileName;
	private String _fileSuffix;

	public HDFSOutput() {

	}

	@Override
	public void init(@SuppressWarnings("rawtypes") Map conf,
			OutputSchema outSchema, String targetName, Period outputPeriod) {
		this._fileName = targetName;
		this.conf = conf;
		this.outputSchema = outSchema;
		this.hdfsHost = Configuration.getString(conf,
				Configuration.OutputHdfsHost);
		this.hdfsPath = Configuration.getString(conf,
				Configuration.OutputHdfsDirectory);
		this.localPath = Configuration.getString(conf,
				Configuration.OutputHdfsLocalDirectory);
		this._fileSuffix = Configuration.getString(conf,
				Configuration.OutputHdfsSuffix);
		this.fieldSeparator = Configuration.getString(conf,
				Configuration.OutputHdfsFieldSeparator);
		this.dataSeparator = Configuration.getString(conf,
				Configuration.OutputHdfsDataSeparator);
		this.compressed = Configuration.getBoolean(conf,
				Configuration.OutputHdfsCompressed);

		createLocalDir(this.localPath);
		if (this._fileSuffix.isEmpty() == false
				&& this._fileSuffix.startsWith(".") == false) {
			this._fileSuffix = "." + this._fileSuffix;
		}
	}

	@Override
	public void write(Calendar periodEnd,
			ISizeAwareIterable<List<Object>> tuples) {
		final String localPath = this.getLocalPath(periodEnd);
		createLocalDir(localPath);

		final String localFileName = Path.combine(localPath, this.getFileName());
		final File file = new File(localFileName);
		if (this.compressed == true) {
			this.writeCompressFile(file, tuples);
		} else {
			this.writeNormalFile(file, tuples);
		}
		String hdfsFile =
				Path.combine((String) System.getenv(com.pplive.pike.Configuration.PIKE_CONF_DIR_KEY), "output-hdfs-site.xml");
		FileSystem fs = null;
		try {
			org.apache.hadoop.conf.Configuration conf =  new org.apache.hadoop.conf.Configuration();
			conf.addResource(hdfsFile);
			fs = FileSystem.get(URI.create(this.hdfsHost), conf);
			copyLocalToHdfs(fs, localFileName, periodEnd);
		} catch (IOException e) {
			log.error("open fileSystem fail:" + this.hdfsHost, e);
			throw new RuntimeException("open fileSystem fail:" + this.hdfsHost,
					e);
		} finally {
			try {
				if (fs != null) {
					fs.close();
				}
			} catch (IOException e) {
				log.error("close hdfs fileSystem failed:" + this.hdfsHost, e);
			}
		}
		deleteCurrentPeriod(file);
	}

	private void writeCompressFile(File file,
			ISizeAwareIterable<List<Object>> tuples) {
		ZipOutputStream output = null;
		try {
			output = openZipOutputStream(file);
			writeHeader(file, output);
			for (List<Object> t : tuples) {
				write(file, output, t);
			}
		} finally {
			try {
				if (output != null) {
					output.close();
				}
			} catch (IOException e) {
				String msg = "close file failed: " + file.getAbsolutePath();
				log.error(msg, e);
			}
		}
	}

	private void writeNormalFile(File file,
			ISizeAwareIterable<List<Object>> tuples) {
		BufferedWriter writer = null;
		try {
			writer = openBufferedWriter(file);
			writeHeader(file, writer);
			for (List<Object> t : tuples) {
				write(file, writer, t);
			}
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				String msg = "close file failed: " + file.getAbsolutePath();
				log.error(msg, e);
			}
		}
	}

	private String getLocalPath(Calendar periodEnd) {
		return Path.combine(Path.combine(this.localPath, this._fileName),
				this.getPeriodString(periodEnd));
	}

	private String getPeriodString(Calendar periodEnd) {
		String outputDate = new SimpleDateFormat("yyyyMMddHHmmss")
				.format(periodEnd.getTime());
		return "dt=" + outputDate;
	}

	private String getHdfsPath(Calendar periodEnd) {
		return Path.combine(Path.combine(this.hdfsPath, this._fileName),
				this.getPeriodString(periodEnd));
	}

	private String getFileName() {
		if (this.compressed == true) {
			return this._fileName + this._fileSuffix + ".zip";
		} else {
			return this._fileName + this._fileSuffix;
		}
	}

	private static void createHdfsDir(FileSystem fs,
			org.apache.hadoop.fs.Path path) throws IOException {
		if (fs.exists(path) == false) {
			createHdfsDir(fs, path.getParent());
			log.info(String.format("%s missing...create it", path.toString()));
			fs.mkdirs(path);
		}
	}

	private void copyLocalToHdfs(FileSystem fs, String localFilePath,
			Calendar periodEnd) {
		String hdfsPath = this.getHdfsPath(periodEnd);
		org.apache.hadoop.fs.Path distPath = new org.apache.hadoop.fs.Path(
				hdfsPath);
		try {
			createHdfsDir(fs, distPath);
			fs.copyFromLocalFile(true, new org.apache.hadoop.fs.Path(
					localFilePath), distPath);
/*			String cmd = String.format("hadoop fs -mkdir -p %s", distPath);
			Runtime.getRuntime().exec(cmd);			
			cmd = String.format("hadoop fs -copyFromLocal %s %s", localFilePath, distPath);
			Runtime.getRuntime().exec(cmd);	*/		
		} catch (IOException e) {
			log.error("copy local file to hdfs fail.file:" + localFilePath, e);
			throw new RuntimeException(e);
		}
	}

	private static void createLocalDir(String path) {
		File file = new File(path);
		if (file.exists() == false) {
			file.mkdirs();
		}
	}

	private void writeHeader(File file, ZipOutputStream writer) {
		assert writer != null;

		Boolean header = Configuration.getBoolean(conf,
				Configuration.OutputRollingHeader, true);
		if (header == false)
			return;

		StringBuffer sb = new StringBuffer();
		for (OutputField f : outputSchema.getOutputFields()) {
			sb.append(f.getName() + this.fieldSeparator);
		}
		sb.append(this.dataSeparator);

		try {
			writer.write(sb.toString().getBytes(ZIP_CHARTSET_NAME));
		} catch (IOException e) {
			String msg = "write tuple to file error: " + file.getAbsolutePath();
			log.error(msg, e);
			throw new RuntimeException(e);
		}
	}

	private void write(File file, ZipOutputStream output, List<Object> tuple) {
		try {
			String s = StringUtils.join(tuple, this.fieldSeparator);
			output.write(s.getBytes(ZIP_CHARTSET_NAME));
			output.write(this.dataSeparator.getBytes(ZIP_CHARTSET_NAME));
		} catch (IOException e) {
			String msg = "write tuple to file error: " + file.getAbsolutePath();
			log.error(msg, e);
			throw new RuntimeException(e);
		}
	}

	private ZipOutputStream openZipOutputStream(File file) {

		ZipOutputStream zipOut;
		try {
			zipOut = new ZipOutputStream(new BufferedOutputStream(
					new FileOutputStream(file)));
			zipOut.putNextEntry(new ZipEntry(file.getName().replace(".zip", "")));
			zipOut.setEncoding(ZIP_CHARTSET_NAME);

			return zipOut;
		} catch (FileNotFoundException e) {
			String msg = "cannot open file for write: "
					+ file.getAbsolutePath();
			log.error(msg, e);
			throw new RuntimeException(msg, e);
		} catch (IOException e) {
			String msg = "open file fail: " + file.getAbsolutePath();
			log.error(msg, e);
			throw new RuntimeException(msg, e);
		} catch (RuntimeException e) {
			log.error("open file file: " + file.getAbsolutePath());
			throw e;
		}

	}

	private static BufferedWriter openBufferedWriter(File file) {
		assert file != null;

		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(file), "utf-8"));
			return writer;
		} catch (UnsupportedEncodingException e) {
			String msg = "should never happen: this JRE/OS does not support UTF8 encoding";
			log.error(msg, e);
			throw new RuntimeException(msg, e);
		} catch (FileNotFoundException e) {
			String msg = "cannot open file for write: "
					+ file.getAbsolutePath();
			log.error(msg, e);
			throw new RuntimeException(msg, e);
		} catch (RuntimeException e) {
			String msg = "open file failed: " + file.getAbsolutePath();
			log.error(msg, e);
			throw e;
		}

	}

	private void writeHeader(File file, Writer writer) {
		assert writer != null;

		StringBuffer sb = new StringBuffer();
		for (OutputField f : this.outputSchema.getOutputFields()) {
			sb.append(f.getName() + this.fieldSeparator);
		}
		sb.append(this.dataSeparator);

		try {
			writer.append(sb.toString());
		} catch (IOException e) {
			String msg = "write tuple to file error: " + file.getAbsolutePath();
			log.error(msg, e);
			throw new RuntimeException(e);
		}
	}

	private void write(File file, Writer writer, List<Object> tuple) {
		try {
			String s = StringUtils.join(tuple, this.fieldSeparator);
			writer.append(s.toString());
			writer.append(this.dataSeparator);
		} catch (IOException e) {
			String msg = "write tuple to file error: " + file.getAbsolutePath();
			log.error(msg, e);
			throw new RuntimeException(e);
		}
	}

	private static void deleteCurrentPeriod(File file) {
		File parentDir = new File(file.getParent());
		parentDir.delete();
	}

	@Override
	public void close() throws IOException {

	}

}
