package com.informatica.vds.custom.sources.ifxcdc;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.*;

import com.informatica.vds.api.IPluginRetryPolicy;
import com.informatica.vds.api.VDSConfiguration;
import com.informatica.vds.api.VDSEventList;
import com.informatica.vds.api.VDSSource;
import com.informatica.vds.custom.sources.ifxcdc.IfxCDCShared.*;

/**
 * VDSSource implementation to read CDC data from Informix 11.70 database
 * 
 * @author pyoung
 *
 */
public class InformixCDCSource implements VDSSource {
	
	static Writer m_DebugWriter = null;
	static IfxCDCShared m_SharedInfo = new IfxCDCShared();

	private static final Logger LOG = LoggerFactory.getLogger(InformixCDCSource.class);
	protected IPluginRetryPolicy pluginRetryPolicyHandler;
	public static final String JDBCDRIVER = "com.informix.jdbc.IfxDriver";

	
	boolean m_LoggingEnabled = false;
	boolean m_CaptureStarted = false;

	InformixReaderThread m_CDCReader = null;
	Thread m_CDCReaderThread = null;

	/**
	 * Constructor called by the VDS framework
	 */
	public InformixCDCSource() {
		getCDCInfo();
	}

	/**
	 * Constructor called by the test framework.
	 * <p>
	 * Sets testing mode and an output writer.
	 * 
	 * @param debugmode
	 * @param writer
	 */
	public InformixCDCSource(boolean debugmode, BufferedWriter writer) {
		this();
		m_SharedInfo.STANDALONE_MODE = debugmode;
		m_DebugWriter = writer;
	}

	/**
	 * Called from the VDS framework to end the plugin
	 * operation
	 */
	public void close() throws IOException {

		m_SharedInfo.closeSource = true;
		try {
			try {
				if (m_SharedInfo.m_EventQueue != null) {
					java.util.concurrent.BlockingQueue<ByteBuffer> tempq = m_SharedInfo.m_EventQueue;
					m_SharedInfo.m_EventQueue = null;
					LOG.info("Clearing event queue...");
					tempq.clear();
				}
				if (m_CDCReaderThread != null) {
					Thread tempThread = m_CDCReaderThread;
					m_CDCReaderThread = null;
					LOG.info("Interrupting CDC reader thread...");
					tempThread.interrupt();
				}
			} catch (Exception ex) {
				LOG.error("EventQueue", ex);
				ex.printStackTrace();
			}
			m_CDCReader.cdcClose();

		} catch (Exception ex) {
			LOG.error("InformixCDCSource:close: ", ex);
			throw new IOException(ex.getMessage());
		}
	}

	/**
	 * Open - called from the VDS framework to begin the plugin session.
	 * <p>
	 * VDS configuration comes from the configuration screen.
	 */
	public void open(VDSConfiguration config) throws Exception {
	
		ConfigParameters cdcConfig = m_SharedInfo.m_Config;
	
		m_SharedInfo.m_EventQueue = new LinkedBlockingQueue<>(IfxCDCShared.EVENT_QUEUE_SIZE);
		LOG.info("Starting CDCReader thread...");
		m_CDCReader = new InformixReaderThread(m_SharedInfo);
		m_CDCReaderThread = new Thread(m_CDCReader);
		// This sets the initial configuration values
		initConfig(config, cdcConfig);

		m_CDCReaderThread.start();
	}

	/**
	 * Read function - entry point to read and process data for VDS. Called
	 * repeatedly by the framework.
	 */
	public void read(VDSEventList vdsevents) throws Exception {

		if (m_SharedInfo.closeSource) {
			close();
			Exception e = new Exception("Source closed");
			throw e;
		}
		ByteBuffer msg = m_SharedInfo.m_EventQueue.poll(1, TimeUnit.SECONDS);
		// No message in the event queue
		if (msg == null) {
			return;
		}
		try {
			byte[] data = msg.array();
			if (m_SharedInfo.STANDALONE_MODE) {
				String outstr = new String(data, "UTF-8");
				m_DebugWriter.write(IfxCDCShared.STARTSTR + "\n");
				m_DebugWriter.write(outstr);
				m_DebugWriter.write(IfxCDCShared.ENDSTR + "\n\n");
				m_DebugWriter.flush();
			} else {
				vdsevents.addEvent(data, data.length);
			}
		} catch (Exception ex) {
			LOG.error("addEvent", ex);
			throw ex;
		}
	}

	/**
	 * Retrieve the list of error codes and record types dynamically from the
	 * Informix database.
	 */
	void getCDCInfo() {
		m_SharedInfo.m_ErrorCodes = new SysCDCErrorCodes();
		m_SharedInfo.m_RecTypes = new SysCDCRecTypes();
	}

	/**
	 * 
	 * @param config - configuration object passed by VDS Framework
	 */
	void initConfig(VDSConfiguration config, ConfigParameters cdcConfig) throws Exception
	{
		ServerParameters svrConf = cdcConfig.serverInfo;
		Document doc = null;
		// This sets the initial configuration values
		try {
			cdcConfig.configFile = config.optString("configFileName", "");
			LOG.info("Config File = " + cdcConfig.configFile);
			cdcConfig.serverInfo.username = config.optString("username", "informix");
			cdcConfig.serverInfo.password = config.optString("pswd", "informix");
			CreateDocFromXML configDoc = new CreateDocFromXML(cdcConfig.configFile);
			configDoc.execute();
			doc = configDoc.m_Document;
		} catch (Exception e) {
			LOG.error(String.format("config file <%s>, username <%s>", 
					cdcConfig.configFile, cdcConfig.serverInfo.username));
			throw e;
		}
		Node root = doc.getDocumentElement();
		
		System.out.println("Root node name = " + root.getNodeName());
		if (root.getNodeName().contentEquals("IFX_Connection")) {
			//Hooray!
		}
		
		Node serverNode; 
		NodeList first = root.getChildNodes();
		NodeList tables = null;
		for (int idx = 0; idx < first.getLength(); idx++) {
			serverNode = first.item(idx);
			// Extract Server parameters
			
			switch (serverNode.getNodeName()) {
			case "Addr":
				svrConf.hostNameAddr = serverNode.getTextContent();
				break;
			case "Port":
				svrConf.tcpPort = Integer.parseInt(serverNode.getTextContent());
				break;
			case "ServiceName":
				svrConf.serverName = serverNode.getTextContent();
				break;
			case "CDCName":
				svrConf.cdcName = serverNode.getTextContent();
				break;
			case "Timeout":
				svrConf.timeout = Integer.parseInt(serverNode.getTextContent());
				break;
			case "MaxRecs":
				svrConf.maxrecs = Integer.parseInt(serverNode.getTextContent());
				break;
			case "Tables":
				tables = ((Element)serverNode).getElementsByTagName("Table");
				break;
			default:
				break;
				
			}
		}
		if (tables != null) {
			Element tblElement;
			int numTables = tables.getLength();
			for (int idx = 0; idx < numTables; idx++) {
				Node table = tables.item(idx);
				NodeList tblFields = table.getChildNodes();
				String configStr;
				tblElement = (Element)tblFields;
				TableParameters tblConfig = m_SharedInfo.allocateTableConfig();
				// Get table to monitor
				configStr = tblElement.getElementsByTagName("TableToMonitor").item(0).getTextContent();
				tblConfig.tableToMonitor = configStr;
				// Get TableIdentifier
				configStr = tblElement.getElementsByTagName("TableIdentifier").item(0).getTextContent();
				tblConfig.tableIdentifier = Integer.parseInt(configStr);
				// Get column names
				configStr = tblElement.getElementsByTagName("ColumnNames").item(0).getTextContent();
				tblConfig.columnsToMonitor = configStr;
				// Get column sizes
				configStr = tblElement.getElementsByTagName("ColumnSizes").item(0).getTextContent();
				tblConfig.columnSizes = configStr;
				
				cdcConfig.tablesInfo.add(tblConfig);
			}
		}
	}

	public void setRetryPolicyHandler(IPluginRetryPolicy iPluginRetryPolicyHandler) {
		this.pluginRetryPolicyHandler = iPluginRetryPolicyHandler;
		this.pluginRetryPolicyHandler.setLogger(LOG);
	}
	
}
