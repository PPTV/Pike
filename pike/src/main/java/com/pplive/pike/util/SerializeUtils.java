package com.pplive.pike.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.PropertyException;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;

public class SerializeUtils {
	public static final String CHARSETNAME = "UTF-8";
	
	private SerializeUtils() {
	}

	

	// public static <T> void jsonSerialize(T obj,String filePath){
	//
	// }

	public static <T> void xmlSerialize(T obj, String filePath) {
		Writer writer = null;
		try {
			writer = new OutputStreamWriter(new FileOutputStream(filePath),
					CHARSETNAME); // new FileWriter(file);
			xmlSerialize(obj, writer);
		}catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Property Exception", e);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(String.format("File %s Not Found Exception",filePath), e);
		} finally {
			if(writer != null)
				try {
					writer.close();
				} catch (IOException e) {
					
				}
		}

	}

	public static <T> void xmlSerialize(T obj, File file) {
		Writer writer = null;
		try {
			writer = new OutputStreamWriter(new FileOutputStream(file),
					CHARSETNAME); // new FileWriter(file);
			xmlSerialize(obj, writer);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Property Exception", e);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(String.format("File %s Not Found Exception",file.getPath()), e);
		} finally {
			if(writer != null)
				try {
					writer.close();
				} catch (IOException e) {
					
				}
		}
	}

	public static <T> void xmlSerialize(T obj, Writer writer) {
		if (obj == null || writer == null) {
			throw new IllegalArgumentException("obj or writer cannot be null");
		}
		try {
			JAXBContext context = JAXBContext.newInstance(obj.getClass());
			Marshaller m = context.createMarshaller();

			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			m.setProperty(Marshaller.JAXB_ENCODING, CHARSETNAME);
			m.marshal(obj, writer);
		} catch (PropertyException e) {
			throw new RuntimeException("Property Exception", e);
		} catch (JAXBException e) {
			throw new RuntimeException("JAXB Exception", e);
		}
	}

	public static <T> String xmlSerialize(T obj) {
		if (obj == null) {
			throw new IllegalArgumentException("obj or writer cannot be null");
		}
		StringWriter writer = null;
		try {

			writer = new StringWriter();
			xmlSerialize(obj, writer);
			return writer.toString();
		} finally {
			if (writer != null)
				try {
					writer.close();
				} catch (IOException e) {
					
				}

		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T xmlDeserialize(Class<T> cls, Reader reader) {
		if (cls == null || reader == null) {
			throw new IllegalArgumentException("cls or reader cannot be null");
		}
		JAXBContext context;
		try {
			context = JAXBContext.newInstance(cls);
			Unmarshaller unmarshaller = context.createUnmarshaller();
			return (T) unmarshaller.unmarshal(reader);
		} catch (JAXBException e) {
			throw new RuntimeException("JAXB Exception", e);
		}
	}

	public static <T> T xmlDeserialize(Class<T> cls, String filepath) {
		Reader reader = null;
		try {
			reader = new InputStreamReader(new FileInputStream(filepath),
					CHARSETNAME);
			return xmlDeserialize(cls, reader);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Unsupported Encoding Exception", e);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(String.format("File %s Not Found Exception",filepath), e);
		} finally {
			if (reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					
				}
		}
	}

	public static <T> T xmlDeserialize(Class<T> cls, File file) {
		Reader reader = null;
		try {
			reader = new InputStreamReader(new FileInputStream(file),
					CHARSETNAME);
			return xmlDeserialize(cls, reader);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Unsupported Encoding Exception", e);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(String.format("File %s Not Found Exception",file.getPath()), e);
		} finally {
			if(reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					
				}
		}
	}

	public static <T> T xmlDeserializeContent(Class<T> cls, String xmlContext) {
		Reader reader = new StringReader(xmlContext);
		try {
			return xmlDeserialize(cls, reader);
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				
			}
		}
	}
	
	public static void generateSchema(Class<?> cls,String filename){
		JAXBContext context;
		final List<DOMResult> results = new ArrayList<DOMResult>();
		try {
			context = JAXBContext.newInstance(cls);
			context.generateSchema(new SchemaOutputResolver() {
				
				@Override
				public Result createOutput(String namespaceUri, String suggestedFileName)
						throws IOException {
					DOMResult result = new DOMResult();
					result.setSystemId(suggestedFileName);
		            results.add(result);
		            return result;
				}
			});
			DOMResult domResult = results.get(0);
		    Document doc = (Document) domResult.getNode();
		    Source source = new DOMSource(doc);

	        // Prepare the output file
	        File file = new File(filename);
	        Result result = new StreamResult(file);

	        // Write the DOM document to the file
	        Transformer xformer = TransformerFactory.newInstance().newTransformer();
	        xformer.transform(source, result);
		} catch (JAXBException e) {
			throw new RuntimeException("JAXB Exception", e);
		}
		catch (IOException e) {
			throw new RuntimeException("JAXB Exception", e);
		}
		catch (TransformerConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransformerFactoryConfigurationError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransformerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
