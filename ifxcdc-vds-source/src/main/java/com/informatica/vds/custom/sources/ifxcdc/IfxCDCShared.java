package com.informatica.vds.custom.sources.ifxcdc;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;



class IfxCDCShared {
	
	public static final int DISABLE_LOGGING = 0;
	public static final int ENABLE_LOGGING = 1;

	public static final int EVENT_QUEUE_SIZE = 4096; // Originally 500
	static final String STARTSTR = "<!-- >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> -->";
	static final String ENDSTR = "<!-- <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< -->";

	Connection m_DBConn = null;
	int m_SessionId = -1;
	SysCDCErrorCodes m_ErrorCodes = null;
	SysCDCRecTypes m_RecTypes = null;

	BlockingQueue<ByteBuffer> m_EventQueue;
	boolean closeSource = false;
	boolean STANDALONE_MODE = false;
	ConfigParameters m_Config = new ConfigParameters();
	
	
	TableParameters allocateTableConfig()
	{
		return new TableParameters();
	}
	
	class ConfigParameters {
		String configFile = "";
		ServerParameters serverInfo = new ServerParameters();
		ArrayList<TableParameters> tablesInfo = new ArrayList<TableParameters>();
	}
	
	class ServerParameters {
		String username;
		String password;
		String hostNameAddr;
		int tcpPort;
		String cdcName;
		String serverName;
		int timeout;
		int maxrecs;
	}
	class TableParameters {
		String tableToMonitor;
		String columnsToMonitor;
		String columnSizes;
		int tableIdentifier;
	}
}
