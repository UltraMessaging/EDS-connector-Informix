/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.*;
import java.util.Calendar;
import java.text.DateFormat; //import java.io.*;

import com.informatica.presales.sources.informix.cdc.InformixCDCSource;
import com.informatica.presales.sources.informix.cdc.IfxCDCShared;
import com.informatica.vds.api.*;

import java.util.Map;
import java.util.HashMap;
import java.lang.Integer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.openmdx.uses.gnu.getopt.*;
import com.informatica.vds.api.VDSEvent;
import com.informatica.vds.api.VDSEventList;

/**
 *
 * @author pyoung
 */
public class SourceTest {
	public static final int MAX_READS=120;
	private static final Logger LOG = LoggerFactory.getLogger(SourceTest.class);
	class MyVDSEvents implements VDSEventList {
		
		@Override
		public void addEvent(byte[] bytes, int i) throws Exception {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public void addEvent(VDSEvent event) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public void addAll(java.util.List<VDSEvent> events) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public void addEvent(byte[] bytes, int i, Map<String, String> map) throws Exception {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public VDSEvent createEvent(int i) throws Exception {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public VDSEvent clone(VDSEvent vdse) throws Exception {
			throw new UnsupportedOperationException("Not supported yet.");
		}
	}

	static class MyVDSConfig implements VDSConfiguration {

		public Map <String, String> configValues = new HashMap<String, String>();

		@Override
		public boolean contains(String string) {
			return configValues.containsKey(string);
		}

		@Override
		public boolean getBoolean(String string) throws Exception {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public int getInt(String string) throws Exception {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public long getLong(String string) throws Exception {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public double getDouble(String string) throws Exception {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public String getString(String string) throws Exception {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean optBoolean(String string, boolean bln) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public int optInt(String string, int i) {
			//throw new UnsupportedOperationException("Not supported yet.");
			if (vdsconf.contains(string)) {
				return (new Integer(vdsconf.configValues.get(string)));
			} else {
				return new Integer(i);
			}
		}

		@Override
		public long optLong(String string, long l) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public double optDouble(String string, double d) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public String optString(String string, String string1) {
			//throw new UnsupportedOperationException("Not supported yet.");
			if (vdsconf.contains(string)) {
				return vdsconf.configValues.get(string);
			} else {
				return new String(string1);
			}
		}

		@Override
		public void addListener(VDSConfigurationListener arg0) throws Exception {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public long getUMPSessionId(int arg0, int arg1, String arg2, String arg3) throws Exception {
			throw new UnsupportedOperationException("Not supported yet.");
		}
	};  // End MyVDSConfig
	static MyVDSConfig vdsconf; // = new MyVDSConfig();
	static MyVDSEvents vdsevents; // = new MyVDSConfig();
	static String m_outFileName;
	static int m_TestRows;
	
	public static void main(String args[]) {
		BufferedWriter writer = null;
		String _progname = Thread.currentThread().getStackTrace()[1].getClassName();
		// TestXML.RunTest();

		DateFormat df;
		Calendar cal;
		cal = Calendar.getInstance();
		df = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.FULL);
		InformixCDCSource testsrc;
		try {
			vdsconf = new MyVDSConfig();
			process_cmdline(_progname, args);
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(m_outFileName, true), "utf-8"));
			writer.write("<!-- ********************************************************************************* -->\n");
			writer.write(String.format("<!-- Start application <%s> at %s -->\n", _progname, df.format(cal.getTime())));
			writer.flush();
			testsrc = new InformixCDCSource(true, writer, m_TestRows);
			testsrc.open(vdsconf);
		} catch (Exception e) {
			LOG.error("open", e);
			return;
		}

		while(true) {
			try {
				testsrc.read(vdsevents);
			} catch (Exception e) {
				LOG.error("Read", e);
				break;
			}
		}
		try {
			testsrc.close();
		} catch (Exception e) {
			LOG.error("Close", e);
			return;
		}
	}

	private static void process_cmdline(String _progname, String[] args)
	{
		try {
			int argId;
			LongOpt[] longopts = { // Name, has_arg, flag, value
					new LongOpt("configFileName", LongOpt.REQUIRED_ARGUMENT, null, 'c'),
					new LongOpt("username", LongOpt.REQUIRED_ARGUMENT, null, 'u'),
					new LongOpt("pswd", LongOpt.REQUIRED_ARGUMENT, null, 'p'),
					new LongOpt("outputFile", LongOpt.REQUIRED_ARGUMENT, null, 'o'),
					new LongOpt("sampleRows", LongOpt.REQUIRED_ARGUMENT, null, 'r')
			};
//			String optarg;

			Getopt g = new Getopt(_progname, args, "c:u:p:o:r:", longopts);
			g.setOpterr(true); // Let getopt handle errors

			while ((argId = g.getopt()) != -1) {
				switch (argId) {
				case 'c':
					vdsconf.configValues.put("configFileName", g.getOptarg());
					break;
				case 'u':
					// username
					vdsconf.configValues.put("username", g.getOptarg());
					break;
				case 'p':
					// pswd
					vdsconf.configValues.put("pswd", g.getOptarg());
					break;
				case 'o':
					m_outFileName = g.getOptarg();
					break;
				case 'r':
					m_TestRows = Integer.parseInt(g.getOptarg());
					break;
				case ':':
					// Missing option arg
					Usage.print(_progname);
					throw new Exception("Missing argument");
					// break;
				case '?':
					// Unknown option
					Usage.print(_progname);
					throw new Exception("Unknown option");
					// break;
				default:
					System.err.println("Unexpected getopt error: <" + argId + ">");
					System.exit(1);
					break;
				} // switch (argId)
			} // while (argId)
		} catch (Exception e) {
			LOG.error("GetOpt", e);
			System.exit(-1);
		} // try-catch
	}	// process_cmdline
}	// Class Source Test

class Usage
{
	final static String message = 
			"Usage: %s [-cup]\n"
					+"\t--configFileName=<file> # XML Configuration File\n"
					+"\t--username=<user> # Informix username (informix)\n"
					+"\t--pswd=<password> # User's password (informix)\n"
					+ "\n"
					+ "";

	static void print(String progname)
	{
		System.err.printf(String.format(Usage.message, progname));
		System.exit(1);
	}
} // Class Usage


//class TestXML
//{
//	static void RunTest() 
//	{
//		try {
//			DocumentBuilderFactory dbFactory =
//					DocumentBuilderFactory.newInstance();
//			DocumentBuilder dBuilder = 
//					dbFactory.newDocumentBuilder();
//			Document doc = dBuilder.newDocument();
//			// root element
//			Element rootElement = doc.createElement("cars");
//			doc.appendChild(rootElement);
//
//			//  supercars element
//			Element supercar = doc.createElement("supercars");
//			rootElement.appendChild(supercar);
//
//			// setting attribute to element
//			Attr attr = doc.createAttribute("company");
//			attr.setValue("Ferrari");
//			supercar.setAttributeNode(attr);
//
//			// carname element
//			Element carname = doc.createElement("carname");
//			Attr attrType = doc.createAttribute("type");
//			attrType.setValue("formula one");
//			carname.setAttributeNode(attrType);
//			carname.appendChild(
//					doc.createTextNode("Ferrari 101"));
//			supercar.appendChild(carname);
//
//			Element carname1 = doc.createElement("carname");
//			Attr attrType1 = doc.createAttribute("type");
//			attrType1.setValue("sports");
//			carname1.setAttributeNode(attrType1);
//			carname1.appendChild(
//					doc.createTextNode("Ferrari 202"));
//			supercar.appendChild(carname1);
//
//			// write the content into xml file
//			TransformerFactory transformerFactory =
//					TransformerFactory.newInstance();
//			Transformer transformer =
//					transformerFactory.newTransformer();
//			DOMSource source = new DOMSource(doc);
//			StreamResult result =
//					new StreamResult(new File("TestXML.xml"));
//			transformer.transform(source, result);
//			// Output to console for testing
//			StreamResult consoleResult =
//					new StreamResult(System.out);
//			transformer.transform(source, consoleResult);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//}