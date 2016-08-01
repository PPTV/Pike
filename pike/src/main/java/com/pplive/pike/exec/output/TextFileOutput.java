package com.pplive.pike.exec.output;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.pplive.pike.base.Period;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.pplive.pike.Configuration;
import com.pplive.pike.base.ISizeAwareIterable;
import com.pplive.pike.util.Path;

class TextFileOutput implements IPikeOutput {

	public static final Logger log = LoggerFactory.getLogger(TextFileOutput.class);
	
	@SuppressWarnings("rawtypes")
	private Map _conf;
	private String _fileDir;
	private String _fileSuffix;
	private boolean _singlefile;
	
	private OutputSchema _outputSchema;
	private String _fileName;
	
	public TextFileOutput() {
	}
	
	@Override
	public void init(@SuppressWarnings("rawtypes") Map conf, OutputSchema outSchema,String targetName, Period outputPeriod) {
		this._fileName = targetName;
		this._conf = conf;
		this._outputSchema = outSchema;
		this._fileDir = Configuration.getString(conf, Configuration.OutputFSDirectory);
		this._fileSuffix = Configuration.getString(conf, Configuration.OutputFileSuffix);
		this._singlefile = Configuration.getBoolean(conf, Configuration.OutputFileSingle);
		
		mkdir(this._fileDir);
		if (this._fileSuffix.isEmpty() == false && this._fileSuffix.startsWith(".") == false) {
			this._fileSuffix = "." + this._fileSuffix;
		}
	}

	@Override
	public void close() throws IOException {
	}
	
	@Override
	public void write(Calendar periodEnd, ISizeAwareIterable<List<Object>> tuples) {
		final String fpath = getPath();
		final String ftemppath = fpath + ".tmp";
		final boolean append = this._singlefile;
		final File file = append ? new File(fpath) : new File(ftemppath);
		BufferedWriter writer = null;
		try {
			writer = openBufferedWriter(file, append);
			writeHeader(file, writer, append);
			for(List<Object> t : tuples){
				write(file, writer, t);
			}
		}
		finally {
			try {
				if (writer != null) { writer.close(); }
			}
			catch(IOException e) {
				String msg = "close file failed: " + file.getAbsolutePath();
				log.error(msg, e);
			}
		}
		
		if (append == false) {
			final File finalFile = new File(fpath);
			try {
				Files.move(file,  finalFile);
			} 
			catch (IOException e) {
				String msg = String.format("rename file failed: %s -> %s", file.getAbsolutePath(), finalFile.getName());
				log.error(msg, e);
				throw new RuntimeException(msg, e);
			}
			createDoneMarkFile(finalFile);
		}
	}
	
	private String getPath(){
		if (this._singlefile) {
			String f = Path.combine(this._fileDir, this._fileName);
			return f + this._fileSuffix;
		}
		
		final String fileName = String.format("%s-%s%s", this._fileName, getNowTimeString(), this._fileSuffix);
		String filePath = Path.combine(this._fileDir, fileName);	
		return filePath;
	}
	
	private static String getNowTimeString() {
		return new SimpleDateFormat("yyyyMMdd_HHmmss_SSS").format(new Date());
	}
	
	private static void mkdir(String path) {
		File file = new File(path);
		if(file.exists() == false)
			file.mkdirs();
	}

	private static BufferedWriter openBufferedWriter(File file, boolean append){
		assert file != null;
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(file, append), "utf-8"));
			return writer;
		} catch (UnsupportedEncodingException e) {
			String msg = "should never happen: this JRE/OS does not support UTF8 encoding";
			log.error(msg, e);
			throw new RuntimeException(msg, e);
		} catch (FileNotFoundException e) {
			String msg = "cannot open file for write: " + file.getAbsolutePath();
			log.error(msg, e);
			throw new RuntimeException(msg, e);
		} catch (RuntimeException e) {
			String msg = "open file failed: " + file.getAbsolutePath();
			log.error(msg, e);
			throw e;
		} 
		
	}
	
	private void writeHeader(File file, Writer writer, boolean append){
		assert writer != null;
		
		if (append) {
			Boolean header = Configuration.getBoolean(_conf, Configuration.OutputRollingHeader, true);
			if(header == false)
				return;
		}

		StringBuffer sb = new StringBuffer();
		for(OutputField f : _outputSchema.getOutputFields()){
			sb.append(f.getName() + "\t");
		}
		sb.append("\n");
		
		try {
			writer.append(sb.toString());
		}
		catch (IOException e) {
			String msg = "write tuple to file error: " + file.getAbsolutePath();
			log.error(msg, e);
			throw new RuntimeException(e);
		}
	}
	
	private static void write(File file, Writer writer, List<Object> tuple) {
		try {
			String s = StringUtils.join(tuple, "\t");
			writer.append(s.toString());
			writer.append("\n");
		}
		catch (IOException e) {
			String msg = "write tuple to file error: " + file.getAbsolutePath();
			log.error(msg, e);
			throw new RuntimeException(e);
		}
	}
	
	private static void createDoneMarkFile(File file) {
		File doneFile = new File(file.getAbsolutePath() + ".done");
		try {
			new FileOutputStream(doneFile).close();
		}
		catch (FileNotFoundException e) {
			String msg = "cannot open file for write: " + file.getAbsolutePath();
			log.error(msg, e);
			throw new RuntimeException(msg, e);
		} 
		catch(IOException e) {
			String msg = "close file failed: " + file.getAbsolutePath();
			log.error(msg, e);
		}
	}

	

}
