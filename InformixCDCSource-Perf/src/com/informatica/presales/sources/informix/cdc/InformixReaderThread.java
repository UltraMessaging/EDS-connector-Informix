package com.informatica.presales.sources.informix.cdc;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.CallableStatement;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informatica.presales.sources.informix.cdc.IfxCDCShared.*;
import com.informix.jdbc.IfxSmartBlob;
import com.informix.lang.IfxToJavaType;

/**
 * 
 * @author pyoung
 *
 *         InformixReaderThread creates a separate thread to receive data from
 *         the CDC API. The data is formatted as CSV and placed in the event
 *         queue to be picked up by main plug-in thread.
 *
 */
class InformixReaderThread implements Runnable {
	public static final int BYTES_TO_READ = 8192;
	/**
	 * Maximum bytes to read - should exceed the largest record received from
	 * CDC API
	 */

	private static final Logger LOG = LoggerFactory.getLogger(InformixReaderThread.class);

	static final DateFormat m_InternalDateFormat = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.FULL);
	static final SimpleDateFormat m_OutputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	public static final String JDBCDRIVER = "com.informix.jdbc.IfxDriver";

	// boolean m_LoggingEnabled = false;
	// boolean m_CaptureStarted = false;
	IfxCDCShared m_SharedInfo;

	HashMap<Integer, TableState> tableStates = new HashMap<Integer, TableState>();
	HashMap<Integer, ArrayList<String>> transactionMap = new HashMap<Integer, ArrayList<String>>();

	InformixReaderThread(IfxCDCShared shared) throws Exception {
		m_SharedInfo = shared;

	}

	/**
	 * Run entry point of the CDCReader Thread object.
	 * <p>
	 * Loops through the data sent from Informix CDC API, extracts the header
	 * data, and calls the handler for each record type.
	 */
	@Override
	public void run() {

		byte[] byteBuffer = new byte[BYTES_TO_READ];
		byte[] dataBuffer = new byte[BYTES_TO_READ * 4];

		int bytesRead = 0;
		int bytesInBuff = 0;
		int bytesProcessed = 0;
		int bytesLeftOver = 0;
		String outputString = "";

		LOG.info("CDCReaderThread running");
		try {
			cdcOpen(m_SharedInfo.m_Config);
			IfxSmartBlob smb = new IfxSmartBlob(m_SharedInfo.m_DBConn);

			while (!m_SharedInfo.closeSource) {

				if (bytesLeftOver > 0) {
					// Save leftover bytes
					System.arraycopy(dataBuffer, bytesInBuff - bytesLeftOver, dataBuffer, 0, bytesLeftOver);
				}
				bytesInBuff = bytesLeftOver;
				// NOTE: IfxLoRead appears to be uninterruptible. May remain here until timeout expires
				bytesRead = smb.IfxLoRead(m_SharedInfo.m_SessionId, byteBuffer, BYTES_TO_READ);
				if (m_SharedInfo.closeSource) {
					break;
				}
				System.arraycopy(byteBuffer, 0, dataBuffer, bytesLeftOver, bytesRead);

				DataInputStream dis = new DataInputStream(new ByteArrayInputStream(dataBuffer));

				LOG.debug("number of bytes read: " + bytesRead);

				if (bytesRead > 0) {

					bytesInBuff += bytesRead;
					bytesLeftOver = bytesInBuff;

					while (true) {

						// Make sure enough bytes to read the payload size.
						if (bytesLeftOver < 8) {
							// Must be large enough to contain header and
							// payload size
							break;
						}
						int headerSize = dis.readInt();
						bytesProcessed += Integer.SIZE / Byte.SIZE;
						int payloadSize = dis.readInt();
						bytesProcessed += Integer.SIZE / Byte.SIZE;

						if (bytesLeftOver < (headerSize + payloadSize)) {
							// break out of loop if incomplete message.
							break;
						}

						int packetScheme = dis.readInt();
						bytesProcessed += Integer.SIZE / Byte.SIZE;
						int recordNumber = dis.readInt();
						bytesProcessed += Integer.SIZE / Byte.SIZE;

						outputString += headerSize;
						outputString += "," + payloadSize;
						outputString += "," + packetScheme;
						outputString += "," + recordNumber;

						switch (recordNumber) {
						case RecTypes.CDC_REC_BEGINTX:
							bytesProcessed = processRecBeginTX(dis, bytesProcessed, outputString);
							bytesLeftOver -= bytesProcessed;
							bytesProcessed = 0;
							break;
						case RecTypes.CDC_REC_COMMTX:
							bytesProcessed = processRecCommitTX(dis, bytesProcessed, outputString);
							bytesLeftOver -= bytesProcessed;
							bytesProcessed = 0;
							break;
						case RecTypes.CDC_REC_RBTX:
							bytesProcessed = processRecRollbackTX(dis, bytesProcessed, outputString);
							bytesLeftOver -= bytesProcessed;
							bytesProcessed = 0;
							break;

						case RecTypes.CDC_REC_INSERT:
						case RecTypes.CDC_REC_DELETE:
						case RecTypes.CDC_REC_UPDBEF:
						case RecTypes.CDC_REC_UPDAFT:
							bytesProcessed = processRecIUD(dis, bytesProcessed, payloadSize, recordNumber,
									outputString);
							bytesLeftOver -= bytesProcessed;
							bytesProcessed = 0;

							break;
						case RecTypes.CDC_REC_TABSCHEMA:
							bytesProcessed = processRecTabSchema(dis, bytesProcessed, payloadSize, outputString);
							bytesLeftOver -= bytesProcessed;
							bytesProcessed = 0;
							break;
						case RecTypes.CDC_REC_TRUNCATE:
							bytesProcessed = processRecTruncate(dis, bytesProcessed, outputString);
							bytesLeftOver -= bytesProcessed;
							bytesProcessed = 0;
							break;

						case RecTypes.CDC_REC_TIMEOUT:
						case RecTypes.CDC_REC_ERROR:
						default:
							bytesProcessed = processUnknownRec(dis, bytesProcessed, payloadSize, headerSize,
									outputString);
							bytesLeftOver -= bytesProcessed;
							bytesProcessed = 0;
							break;
						} // switch recordNumber
						outputString = "";

					} // while dataToProcess
				} // if bytesRead > 0
				dis.close();
				bytesProcessed = 0;
			} // while !endOfRead
		} catch (Exception ex) {
			LOG.error("Exception", ex);
			ex.printStackTrace();
		}
		try {
			m_SharedInfo.closeSource = true;
			//Thread.sleep(1000);
		} catch (Exception e) {
			LOG.error("close+sleep", e);
		}
		return;
	} // run

	void cdcOpen(ConfigParameters cdcConfig) throws Exception {

		int retval = -1;

		ServerParameters server = cdcConfig.serverInfo;
		connect(server.hostNameAddr, server.tcpPort, server.cdcName, server.serverName, server.username,
				server.password);
		m_SharedInfo.m_EventQueue = new LinkedBlockingQueue<>(IfxCDCShared.EVENT_QUEUE_SIZE);

		m_SharedInfo.m_SessionId = opensess(server.serverName, 0, server.timeout, server.maxrecs, 1, 1);
		if (m_SharedInfo.m_SessionId < 0) {
			String errmsg = "Unable to open session.  returned sessionId value: " + m_SharedInfo.m_SessionId;
			LOG.error(errmsg);
			Exception e = new Exception(errmsg);
			throw e;
		}
		for (int idx = 0; idx < cdcConfig.tablesInfo.size(); idx++) {
			TableState tblState = new TableState();
			tblState.tableToMonitor = cdcConfig.tablesInfo.get(idx).tableToMonitor;
			tblState.columnSizes = cdcConfig.tablesInfo.get(idx).columnSizes;
			tblState.columnsToMonitor = cdcConfig.tablesInfo.get(idx).columnsToMonitor;
			tblState.tableIdentifier = cdcConfig.tablesInfo.get(idx).tableIdentifier;
			tableStates.put(tblState.tableIdentifier, tblState);

			retval = setfullrowlogging(tblState.tableToMonitor, IfxCDCShared.ENABLE_LOGGING);
			if (retval >= 0) {
				tblState.loggingEnabled = true;
				LOG.info("setfullrowlogging enabled for " + tblState.tableToMonitor);
			} else {
				LOG.info(errortext(retval));
				cdcClose();
				throw new Exception(errortext(retval));
			}
			retval = startcapture(m_SharedInfo.m_SessionId, 0, tblState.tableToMonitor, tblState.columnsToMonitor,
					tblState.tableIdentifier);
			if (retval >= 0) {
				tblState.captureStarted = true;
				LOG.info("startcapture enabled for " + tblState.tableToMonitor + " on columns "
						+ tblState.columnsToMonitor);
			} else {
				LOG.error(errortext(retval));
				cdcClose();
				throw new Exception(errortext(retval));
			}
		}
		// TODO - PASS LSN Value to recover or restart CDC stream
		retval = activatesess(m_SharedInfo.m_SessionId, 0);
		if (retval >= 0) {
			LOG.info("activatesess enabled for sessionId " + m_SharedInfo.m_SessionId);
		} else {
			LOG.error(errortext(retval));
			cdcClose();
			throw new Exception(errortext(retval));
		}
		LOG.info("CDC started...");
	}

	void cdcClose() throws IOException {

		m_SharedInfo.closeSource = true;
		try {
			try {
				LOG.info("Clearing event queue...");
				if (m_SharedInfo.m_EventQueue != null) {
					java.util.concurrent.BlockingQueue<ByteBuffer> tempq = m_SharedInfo.m_EventQueue;
					m_SharedInfo.m_EventQueue = null;
					LOG.info("Clearing event queue...");
					tempq.clear();
				}
			} catch (Exception ex) {
				LOG.error("EventQueue", ex);
				ex.printStackTrace();
			}
			
			Iterator<Map.Entry<Integer,InformixReaderThread.TableState>> it = 
					tableStates.entrySet().iterator();
			while (it.hasNext()) {
				TableState tblState = it.next().getValue();

				if (tblState.captureStarted) {
					endcapture(m_SharedInfo.m_SessionId, 0, tblState.tableToMonitor, tblState.tableIdentifier);
					tblState.captureStarted = false;
				}
				// TODO - leave full row logging on to allow recovery between source runs.
				if (tblState.loggingEnabled) {
					setfullrowlogging(tblState.tableToMonitor, IfxCDCShared.DISABLE_LOGGING);
					tblState.loggingEnabled = false;
				}
				it.remove();
			}

			if (m_SharedInfo.m_SessionId >= 0) {
				int tempId = m_SharedInfo.m_SessionId;
				m_SharedInfo.m_SessionId = -1;
				closesess(tempId);
			}
			if (m_SharedInfo.m_DBConn != null) {
				java.sql.Connection tempConn = m_SharedInfo.m_DBConn;
				m_SharedInfo.m_DBConn = null;
				if (!tempConn.isClosed()) {
					tempConn.close();
					LOG.info("Closed database connection.");
				}
			}
		} catch (Exception ex) {
			LOG.error("InformixReaderThread:cdcClose: ", ex);
			throw new IOException(ex.getMessage());
		}
	}

	/**
	 * Processes a Begin Transaction message from CDC API.
	 * <p>
	 * First, makes sure the transaction ID does not already exist in the
	 * transaction map. Starts a new transaction list inside m_TransactionMap.
	 * <p>
	 * If STANDALONE_MODE is true, generates an CSV representation which will be
	 * shown in the debug output. Otherwise, no output is generated.
	 * 
	 * @param dis
	 *            DataInputStream - the input stream
	 * @param bytesProcessed
	 *            - The number of bytes already processed in the input stream
	 * @param outStr
	 *            - Output String
	 * @return bytesProcessed - the number of bytes consumed in the
	 * @throws Exception
	 */
	int processRecBeginTX(DataInputStream dis, int bytesProcessed, String outStr) throws Exception {
		long seqNumber = dis.readLong();
		bytesProcessed += Long.SIZE / Byte.SIZE;
		int trxId = dis.readInt();
		bytesProcessed += Integer.SIZE / Byte.SIZE;
		long startTime = dis.readLong();
		bytesProcessed += Long.SIZE / Byte.SIZE;
		int userId = dis.readInt();
		bytesProcessed += Integer.SIZE / Byte.SIZE;

		if (transactionMap.containsKey(trxId)) {
			String errmsg = new String("BeginTX: duplicate transaction <" + trxId + "> !!!");
			LOG.error(errmsg);
			Exception e = new Exception(errmsg);
			throw e;
		}

		transactionMap.put(trxId, new ArrayList<String>());
		if (m_SharedInfo.STANDALONE_MODE) {
			outStr += "," + "BEGINTX";
			outStr += "," + seqNumber;
			outStr += "," + trxId;
			outStr += "," + userId;
			outStr += "," + startTime;
			outStr += ",\"" + m_InternalDateFormat.format(startTime * 1000L) + "\"";
			outStr += "\n";
			if (m_SharedInfo.m_EventQueue != null) {
				m_SharedInfo.m_EventQueue.add(ByteBuffer.wrap(outStr.getBytes()));
			}
			outStr = "";
		}

		return bytesProcessed;
	}

	/**
	 * Processes a Commit Transaction message from CDC API.
	 * <p>
	 * First, makes sure the transaction ID already exists in the transaction
	 * map. Send each individual operation onto m_EventQueue to be picked up by
	 * the main plug-in thread. Remove operations from the list for this
	 * transaction
	 * <p>
	 * If STANDALONE_MODE is true, generates an XML representation which will be
	 * shown in the debug output. Otherwise, no output is generated.
	 * 
	 * @param dis
	 *            DataInputStream - the input stream
	 * @param bytesProcessed
	 *            - The number of bytes already processed in the input stream
	 * @param outStr
	 *            - Output String
	 * @return bytesProcessed - the number of bytes consumed in this method
	 * @throws Exception
	 */
	int processRecCommitTX(DataInputStream dis, int bytesProcessed, String outStr) throws Exception {
		long seqNumber = dis.readLong();
		bytesProcessed += Long.SIZE / Byte.SIZE;
		int trxId = dis.readInt();
		bytesProcessed += Integer.SIZE / Byte.SIZE;
		long commitTime = dis.readLong();
		bytesProcessed += Long.SIZE / Byte.SIZE;

		String errmsg = null;

		if (!transactionMap.containsKey(trxId)) {
			errmsg = new String("CommitTX: unknown transaction <" + trxId + "> !!!");
		}

		if (errmsg != null) {
			LOG.error(errmsg);
			Exception e = new Exception(errmsg);
			throw e;
		}

		ArrayList<String> list = transactionMap.get(trxId);
		ListIterator<String> iter = list.listIterator();
		while (iter.hasNext()) {
			String next = iter.next();
			// Insert commit time before row data
			int insertTime = next.indexOf('"', 0);
			if (insertTime >= 0) {
				next = next.substring(0, insertTime) + commitTime + "," + next.substring(insertTime);
			} else {
				next += "," + commitTime;
			}
			next += "\n";
			if (m_SharedInfo.m_EventQueue != null) {
				m_SharedInfo.m_EventQueue.add(ByteBuffer.wrap(next.getBytes()));
			}
		}
		int size = list.size();
		for (int i = size - 1; i >= 0; i--) {
			list.remove(i);
		}
		LOG.debug("CommitTX: SENT " + size + " operations");
		transactionMap.remove(trxId);

		if (m_SharedInfo.STANDALONE_MODE) {
			outStr += "," + "COMMTX";
			outStr += "," + seqNumber;
			outStr += "," + trxId;
			outStr += "," + commitTime;
			outStr += ",\"" + m_InternalDateFormat.format(commitTime * 1000L) + "\"";
			outStr += "\n";
			m_SharedInfo.m_EventQueue.add(ByteBuffer.wrap(outStr.getBytes()));
			outStr = "";
		}
		return bytesProcessed;
	}

	/**
	 * Processes a Rollback Transaction message from CDC API.
	 * <p>
	 * First, makes sure the transaction ID already exists in the transaction
	 * map. Remove operations from the list for this transaction.
	 * <p>
	 * If STANDALONE_MODE is true, generates an XML representation which will be
	 * shown in the debug output.
	 * 
	 * @param dis
	 *            DataInputStream - the input stream
	 * @param bytesProcessed
	 *            - The number of bytes already processed in the input stream
	 * @param outStr
	 *            - Output String
	 * @return bytesProcessed - the number of bytes consumed in this method
	 * @throws Exception
	 */
	int processRecRollbackTX(DataInputStream dis, int bytesProcessed, String outStr) throws Exception {
		long seqNumber = dis.readLong();
		bytesProcessed += Long.SIZE / Byte.SIZE;
		int trxId = dis.readInt();
		bytesProcessed += Integer.SIZE / Byte.SIZE;

		if (!transactionMap.containsKey(trxId)) {
			String errmsg = new String("RollbackTX: unknown transaction <" + trxId + "> !!!");
			LOG.error(errmsg);
			Exception e = new Exception(errmsg);
			throw e;
		}
		ArrayList<String> list = transactionMap.get(trxId);
		int size = list.size();
		for (int i = size - 1; i >= 0; i--) {
			list.remove(i);
		}
		transactionMap.remove(trxId);
		LOG.info("RollbackTX: REMOVED " + size + " operations");

		if (m_SharedInfo.STANDALONE_MODE) {
			outStr += "," + "RBTX";
			outStr += "," + seqNumber;
			outStr += "," + trxId;
			outStr += "\n";
			m_SharedInfo.m_EventQueue.add(ByteBuffer.wrap(outStr.getBytes()));
			outStr = "";
		}
		return bytesProcessed;
	}

	/**
	 * Process a change record from the CDC API This handles the Insert, Update,
	 * and Delete operations.
	 * <p>
	 * First, verify that the transaction ID exists in the transaction map
	 * (m_TransactionMap). For an update operation, we will receive both an
	 * update-before and an update-after record. The update before is discarded.
	 * All other transactions are parsed and the individual fields are added to
	 * the "body" section in the output XML.
	 * <p>
	 * The resulting XML string is placed in the m_TransactionMap
	 * 
	 * @param dis
	 *            DataInputStream - the input stream
	 * @param bytesProcessed
	 *            - The number of bytes already processed in the input stream
	 * @param outStr
	 *            - Output String
	 * @return bytesProcessed - the number of bytes consumed in this method
	 * @throws Exception
	 */
	int processRecIUD(DataInputStream dis, int bytesProcessed, int payloadSize, int recordNumber, String outStr)
			throws Exception {
		long seqNumber = dis.readLong();
		bytesProcessed += Long.SIZE / Byte.SIZE;
		int trxId = dis.readInt();
		bytesProcessed += Integer.SIZE / Byte.SIZE;
		int userData = dis.readInt();
		bytesProcessed += Integer.SIZE / Byte.SIZE;
		int flags = dis.readInt();
		bytesProcessed += Integer.SIZE / Byte.SIZE;

		TableState tblState = tableStates.get(userData);
		if (tblState == null) {
			String errmsg = new String("processRecChange: unknown transaction <" + trxId + "> !!!");
			LOG.error(errmsg);
			Exception e = new Exception(errmsg);
			throw e;
		}

		int numVarLengthCols = tblState.cdcTableSchema.numVarCols;
		String recname = "";
		int lengths[] = new int[numVarLengthCols];
		for (int i = 0; i < numVarLengthCols; i++) {
			// read the 4 byte fields for each of
			// the variable column size values...
			// use when the data is actually
			// processed...
			lengths[i] = dis.readInt();
			bytesProcessed += Integer.SIZE / Byte.SIZE;
		}

		byte[] restOfPayload = new byte[payloadSize];
		dis.readFully(restOfPayload, 0, payloadSize);
		bytesProcessed += payloadSize;
		{
			ByteBuffer byteMessage = ByteBuffer.wrap(restOfPayload);
			LOG.debug(tblState.tableToMonitor + ":columns <" + Arrays.toString(byteMessage.array()) + ">");
		}

		switch (recordNumber) {
		case RecTypes.CDC_REC_INSERT:
			recname = "INSERT";
			break;
		case RecTypes.CDC_REC_DELETE:
			recname = "DELETE";
			break;
		case RecTypes.CDC_REC_UPDBEF:
			// recname = "UPDBEF";
			return bytesProcessed;
		// break;
		case RecTypes.CDC_REC_UPDAFT:
			recname = "UPDAFT";
			break;
		}

		if (!transactionMap.containsKey(trxId)) {
			String errmsg = new String(recname + ": Unknown transaction <" + trxId + "> !!!");
			LOG.error(errmsg);
			Exception e = new Exception(errmsg);
			throw e;
		}
		outStr += "," + tblState.tableToMonitor;
		outStr += "," + recname;
		outStr += "," + seqNumber;
		outStr += "," + trxId;
		outStr += "," + userData;
		outStr += "," + flags;
		
		outStr += ",";	// prefix the Columns field

		outStr += createColumnsFields(restOfPayload, lengths, tblState);
		transactionMap.get(trxId).add(outStr);
		return bytesProcessed;
	}

	/**
	 * Process the table schema returned at the start of each session
	 * <p>
	 * The table schema is returned as a comma separated list of field names and
	 * field types.
	 * 
	 * @param dis
	 *            DataInputStream - the input stream
	 * @param bytesProcessed
	 *            - The number of bytes already processed in the input stream
	 * @param outStr
	 *            - Output String
	 * @return bytesProcessed - the number of bytes consumed in this method
	 * @throws Exception
	 */
	int processRecTabSchema(DataInputStream dis, int bytesProcessed, int payloadSize, String outStr) throws Exception {
		int userData = dis.readInt();
		bytesProcessed += Integer.SIZE / Byte.SIZE;
		int flags = dis.readInt();
		bytesProcessed += Integer.SIZE / Byte.SIZE;
		int fixedLengthSize = dis.readInt();
		bytesProcessed += Integer.SIZE / Byte.SIZE;
		int fixedLengthCols = dis.readInt();
		bytesProcessed += Integer.SIZE / Byte.SIZE;
		int varLengthCols = dis.readInt();
		bytesProcessed += Integer.SIZE / Byte.SIZE;

		String strBadColNames = "";
		TableState tblState = tableStates.get(userData);

		{ // Fill the schema structure
			tblState.cdcTableSchema.userData = userData;
			tblState.cdcTableSchema.flags = flags;
			tblState.cdcTableSchema.numFixedBytes = fixedLengthSize;
			tblState.cdcTableSchema.numFixedCols = fixedLengthCols;
			tblState.cdcTableSchema.numVarCols = varLengthCols;
		}
		// use IfxToJavaType class to convert
		// Informix to Java datatypes when actually
		// processing the columns...
		byte[] restOfPayload = new byte[payloadSize];
		dis.readFully(restOfPayload, 0, payloadSize);
		LOG.debug("Columns: (" + new String(restOfPayload) + ")");
		String[] colNamesTypes = new String(restOfPayload).split(",");
		String[] colSizes = new String(tblState.columnSizes.trim()).split(",");
		for (int i = 0; i < colNamesTypes.length; i++) {
			String[] nameType = colNamesTypes[i].trim().split(" ");
			ColumnDef colDef = new ColumnDef();
			colDef.name = nameType[0];
			colDef.dataType = nameType[1].toLowerCase();
			colDef.ifxDataType = ifxTypeNametoEnum(colDef.dataType);
			colDef.index = i;
			if (i < fixedLengthCols) {
				colDef.isFixed = true;
				colDef.size = rTypeSize(colDef);
				if (colDef.size < 0) {
					strBadColNames += colDef.name + ";";
					colDef.size = Integer.parseInt(colSizes[i].trim(), 10);
				}
			} else {
				colDef.isFixed = false;
			}
			if (nameType[1].toLowerCase().startsWith("char") || nameType[1].toLowerCase().startsWith("varchar")) {
				colDef.isString = true;
			} else {
				colDef.isString = false;
			}
			// BLow it up if there are bad column sizes.
			if (strBadColNames.length() > 0) {
				String msg = tblState.tableToMonitor + ": Bad Column lengths: " + strBadColNames;
				LOG.warn(msg);;
				if (m_SharedInfo.STANDALONE_MODE) {
//					Exception e = new Exception (msg);
//					throw e;
				}
			}
			tblState.cdcTableSchema.columns.add(i, colDef);
		}

		bytesProcessed += payloadSize;

		if (m_SharedInfo.STANDALONE_MODE) {
			outStr += "," + "TABSCHEMA";
			outStr += "," + userData;
			outStr += "," + flags;
			outStr += "," + fixedLengthSize;
			outStr += "," + fixedLengthCols;
			outStr += "," + varLengthCols;
			outStr += "," + new String(restOfPayload);

			outStr += "\n";
			m_SharedInfo.m_EventQueue.add(ByteBuffer.wrap(outStr.getBytes()));
			outStr += "";
		}
		

		return bytesProcessed;
	}

	/**
	 * Simulate the CDC C API call 'rtypsize' to return the number of bytes used by the column within the CDC record.
	 * @param colDef
	 * @return
	 */
	int rTypeSize(ColumnDef colDef) {
		int typeSize = 0;
		switch (colDef.ifxDataType) {
		case BIGINT:
			// 2 bytes sign
			// 8 bytes data
			typeSize = 10;

			break;
		case CHARACTER:
			int i1, i2;
			i1 = colDef.dataType.lastIndexOf('(');
			i2 = colDef.dataType.lastIndexOf(')');
			if (i1 < 0 || i2 < 0) {
				// Default of 1 char
				typeSize = 1;
			} else {
				typeSize = Integer.decode(colDef.dataType.substring(i1+1, i2));
			}
			
			break;
		case DATE:
			typeSize = 4;
			break;
		case DOUBLE:
			typeSize = 8;
			break;
		case FLOAT:
			typeSize = 4;
			break;
		case INT8:
			// 2 bytes sign, 8 bytes data
			typeSize = 10;
			break;
		case INTEGER:
			typeSize = 4;
			break;
		case NUMERIC:
			typeSize = 4;
			break;
		case REAL:
			typeSize = 4;
			break;
		case SERIAL:
			typeSize = 4;
			break;
		case SMALLFLOAT:
			typeSize = 2;
			break;
		case SMALLINT:
			typeSize = 2;
			break;
		case VARCHAR:
			// Variable
			typeSize = 0;
			break;
		case UNKNOWN:
		case TEXT:
		case BYTE:
		case DECIMAL:
		case DATETIME:
		case INTERVAL:
		case MONEY:
			// Unknown - return -1
			typeSize = -1;
			break;
		}
		return typeSize;
	}

	/**
	 * Process the Truncate command.
	 * 
	 * @param dis
	 *            DataInputStream - the input stream
	 * @param bytesProcessed
	 *            - The number of bytes already processed in the input stream
	 * @param outStr
	 *            - Output String
	 * @return bytesProcessed - the number of bytes consumed in this method
	 * @throws Exception
	 */
	int processRecTruncate(DataInputStream dis, int bytesProcessed, String outStr) throws Exception {
		long seqNumber = dis.readLong();
		bytesProcessed += Long.SIZE / Byte.SIZE;
		int trxId = dis.readInt();
		bytesProcessed += Integer.SIZE / Byte.SIZE;
		int userData = dis.readInt();
		bytesProcessed += Integer.SIZE / Byte.SIZE;
		TableState tblState = tableStates.get(userData);

		if (tblState == null) {
			String errmsg = new String("processRecTruncate: unknown transaction <" + trxId + "> !!!");
			LOG.error(errmsg);
			Exception e = new Exception(errmsg);
			throw e;
		}

		if (!transactionMap.containsKey(trxId)) {
			String errmsg = new String("TRUNCATE: Unknown transaction <" + trxId + "> !!!");
			LOG.error(errmsg);
			Exception e = new Exception(errmsg);
			throw e;
		}

		outStr += "," + tblState.tableToMonitor;
		outStr += "," + "TRUNCATE";
		outStr += "," + seqNumber;
		outStr += "," + trxId;
		outStr += "," + userData;

		transactionMap.get(trxId).add(outStr);

		return bytesProcessed;
	}

	/**
	 * Process a record with unknown type. Consumes the message and creates an
	 * XML object with the record header and body
	 * 
	 * @param dis
	 *            DataInputStream - the input stream
	 * @param bytesProcessed
	 *            - The number of bytes already processed in the input stream
	 * @param outStr
	 *            - Output String
	 * @return bytesProcessed - the number of bytes consumed in this method
	 * @throws Exception
	 */
	int processUnknownRec(DataInputStream dis, int bytesProcessed, int payloadSize, int headerSize, String outStr)
			throws IOException {

		byte[] restOfPayload = new byte[payloadSize];
		int headerRemainder = headerSize - bytesProcessed;
		if (headerRemainder < 0) {
			headerRemainder = 0;
		}
		byte[] restOfHeader = new byte[headerRemainder];
		dis.readFully(restOfHeader, 0, headerRemainder);
		dis.readFully(restOfPayload, 0, payloadSize);
		bytesProcessed += headerRemainder + payloadSize;

		outStr += "," + "Unknown";
		outStr += "," + new String(restOfHeader);
		outStr += "," + new String(restOfPayload);

		return bytesProcessed;
	}

	/**
	 * Returns a double quote-delimited csv string of all the columns.
	 * 
	 * @param restOfPayload
	 * @param varFieldLengths
	 * @return outStr
	 */
	String createColumnsFields(byte[] restOfPayload, int[] varFieldLengths, TableState tblState) {
		String outStr = ""; 
		int nextValue = 0;
		int totalFields = tblState.cdcTableSchema.numFixedCols + tblState.cdcTableSchema.numVarCols;
		for (int i = 0; i < totalFields; i++) {
			ColumnDef columnDef = tblState.cdcTableSchema.columns.get(i);

			int numBytes;
			if (i < tblState.cdcTableSchema.numFixedCols) {
				numBytes = columnDef.size;
			} else {
				numBytes = varFieldLengths[i - tblState.cdcTableSchema.numFixedCols];
			}
			String strVal = ifxDataToString(Arrays.copyOfRange(restOfPayload, nextValue, nextValue + numBytes),
					numBytes, columnDef.ifxDataType);
			if (i > 0) {
				outStr += "|";
			} 
			outStr += strVal;
			
			nextValue += numBytes;
		}
		outStr = outStr.replaceAll("\"", "\"\"");
		
		return "\"" + outStr + "\"";
	}

	/**
	 * Returns an internal type enumeration based on an input data type string
	 * from the TableSchema sent by the CDC API.
	 * 
	 * @param dataType
	 *            - Datatype field extracted from TableSchema record.
	 * @return Enumeration for data type.
	 */
	IfxType ifxTypeNametoEnum(String dataType) {
		if (dataType.startsWith("bigint")) {
			return IfxType.BIGINT;
		}
		if (dataType.startsWith("byte")) {
			return IfxType.BYTE;
		}
		if (dataType.startsWith("char")) {
			return IfxType.CHARACTER;
		}
		if (dataType.startsWith("datetime")) {
			return IfxType.DATETIME;
		}
		if (dataType.startsWith("date")) {
			return IfxType.DATE;
		}
		if (dataType.startsWith("decimal")) {
			return IfxType.DECIMAL;
		}
		if (dataType.startsWith("double")) {
			return IfxType.DOUBLE;
		}
		if (dataType.startsWith("float")) {
			return IfxType.FLOAT;
		}
		if (dataType.startsWith("int8")) {
			return IfxType.INT8;
		}
		if (dataType.startsWith("integer")) {
			return IfxType.INTEGER;
		}
		if (dataType.startsWith("interval")) {
			return IfxType.INTERVAL;
		}
		if (dataType.startsWith("money")) {
			return IfxType.MONEY;
		}
		if (dataType.startsWith("numeric")) {
			return IfxType.NUMERIC;
		}
		if (dataType.startsWith("real")) {
			return IfxType.REAL;
		}
		if (dataType.startsWith("serial")) {
			return IfxType.SERIAL;
		}
		if (dataType.startsWith("smallfloat")) {
			return IfxType.SMALLFLOAT;
		}
		if (dataType.startsWith("smallint")) {
			return IfxType.SMALLINT;
		}
		if (dataType.startsWith("text")) {
			return IfxType.TEXT;
		}
		if (dataType.startsWith("varchar")) {
			return IfxType.VARCHAR;
		}

		return IfxType.UNKNOWN;
	}

	/**
	 * Convert binary data to a string representation for writing into an XML or
	 * CSV field. Strings are delimited by single quotes
	 * 
	 * @param data
	 *            - Input byte array with data
	 * @param datalen
	 *            - Number of bytes in the data.
	 * @param type
	 *            - Enumeration describing the data type.
	 * @return String representation of the input data.
	 */
	String ifxDataToString(byte[] data, int datalen, IfxType type) {
		String outString = null;
		switch (type) {
		case BIGINT: {
			long value = IfxToJavaType.IfxToJavaLongBigInt(data);
			outString = String.format("%d", value);
			break;
		}
		case CHARACTER: {
			try {
				outString = new IfxToJavaType().IfxToJavaChar(data, true);
				outString = "'" + outString.replace("'", "''") + "'";
			} catch (Exception e) {
				LOG.error("Convert CHAR", e);
				outString = e.getMessage();
			}
			break;
		}
		case INTERVAL:
		case DATETIME: { // TODO - verify precision handling
			java.sql.Timestamp ts = IfxToJavaType.IfxToJavaDateTime(data, (short) 3);
			outString = m_OutputDateFormat.format(ts);
			break;
		}
		case DATE: {
			java.sql.Date date = IfxToJavaType.IfxToJavaDate(data);
			outString = m_OutputDateFormat.format(date);
			break;
		}
		case NUMERIC:
		case MONEY:
		case DECIMAL: { // TODO - get scale and precision from schema record
			DecimalFormat df = new DecimalFormat();
			java.math.BigDecimal big = IfxToJavaType.IfxToJavaDecimal(data, (short) 0);
			outString = df.format(big);
			break;
		}
		case FLOAT:
		case DOUBLE: {
			double dbl = IfxToJavaType.IfxToJavaDouble(data);
			outString = String.format("%f", dbl);
			break;
		}
		case INT8: {
			long value = IfxToJavaType.IfxToJavaLongInt(data);
			outString = String.format("%d", value);
			break;
		}
		case SERIAL:
		case INTEGER: {
			int value = IfxToJavaType.IfxToJavaInt(data);
			outString = String.format("%d", value);
			break;
		}
		case SMALLFLOAT:
		case REAL: {
			float value = IfxToJavaType.IfxToJavaReal(data);
			outString = String.format("%f", value);
			break;
		}
		case SMALLINT: {
			short value = IfxToJavaType.IfxToJavaSmallInt(data);
			outString = String.format("%d", value);
			break;
		}
		case VARCHAR: {
			outString = new String(data, 1, datalen - 1);
			outString = "'" + outString.replace("'", "''") + "'";
			break;
		}
		case BYTE:
		case TEXT:
		case UNKNOWN:
			// default:
			outString = "";
			break;
		}
		return outString;
	}

	/**
	 * Call the Informix activate session API function.
	 * 
	 * @param sessionId
	 *            - The session ID returned from Open Session call.
	 * @param cdcSeq
	 *            - Should contain the starting LSN
	 * @return retval - return value from the CallableStatement
	 * @throws SQLException
	 */
	public int activatesess(int sessionId, long cdcSeq) throws SQLException {

		int retval = -1;
		CallableStatement cs = null;

		LOG.info(String.format("Executing activatesess with parameters: sessionId = %d, cdcSeq = %d", sessionId,
				cdcSeq));

		try {
			cs = m_SharedInfo.m_DBConn.prepareCall("{ ? = call informix.cdc_activatesess(?, ?) }");
			validateCallableStatement(cs);

			cs.setInt(1, sessionId);
			cs.setLong(2, cdcSeq);

			ResultSet rs = cs.executeQuery();
			if (rs != null) {
				rs.next();
				retval = rs.getInt(1);
			}
		} catch (SQLException ex) {
			LOG.error("SQLError", ex);
			ex.printStackTrace();
			throw ex;
		} finally {
			if (cs != null) {
				cs.close();
			}
		}
		return retval;
	}

	/**
	 * Closes the current session.
	 * 
	 * @param sessionId
	 *            - Session ID returned from the Open session call.
	 * @return return value from the callable statement
	 * @throws SQLException
	 */
	public int closesess(int sessionId) throws SQLException {

		int retval = -1;
		CallableStatement cs = null;

		LOG.info(String.format("Executing closesess with parameters: sessionId = %d", sessionId));

		try {
			cs = m_SharedInfo.m_DBConn.prepareCall("{ ? = call informix.cdc_closesess(?) }");
			validateCallableStatement(cs);

			cs.setInt(1, sessionId);
			ResultSet rs = cs.executeQuery();
			if (rs != null) {
				rs.next();
				retval = rs.getInt(1);
			}
		} catch (SQLException ex) {
			LOG.error("SQLerror", ex);
			ex.printStackTrace();
			throw ex;
		} finally {
			if (cs != null) {
				cs.close();
				m_SharedInfo.m_SessionId = 0;
			}
		}
		return retval;
	}

	/**
	 * Create the JDBC connection to the Informix Database and retrieve the
	 * error codes and record types
	 * 
	 * @param host
	 *            - Hostname of the database server
	 * @param port
	 *            - TCP port for JDBC connection
	 * @param database
	 *            - Database name (e.g. syscdcv1)
	 * @param iServer
	 *            - Informix server name (e.g. ol_informix1170)
	 * @param user
	 *            - Informix Login username (e.g. informix)
	 * @param pswd
	 *            - Informix Login password
	 * @throws Exception
	 */
	public void connect(String host, int port, String database, String iServer, String user, String pswd)
			throws Exception {

		// sample:
		// jdbc:informix-sqli://10.6.1.9:1512/prod28:INFORMIXSERVER=hp17_jdevolo_tcp;DB_LOCALE=i
		// w_il.8859-8;CLIENT_LOCALE=iw_il.8859-8;IFX_ISOLATION_LEVEL=3U;IFX_LOCK_MODE_WAIT=10
		String connStr = String.format("jdbc:informix-sqli://%s:%d/%s:INFORMIXSERVER=%s;user=%s;password=%s", host,
				port, database, iServer, user, pswd);
		String logStr = String.format("jdbc:informix-sqli://%s:%d/%s:INFORMIXSERVER=%s;user=%s;password=%s", host,
				port, database, iServer, user, "*****");

		LOG.info("Loading jdbc driver to connect via url: " + logStr);
		Class.forName(JDBCDRIVER);

		LOG.info("Creating connection to database...");
		m_SharedInfo.m_DBConn = DriverManager.getConnection(connStr);
		LOG.info("Database connection created.");

		m_SharedInfo.m_ErrorCodes.loadSysCDCErrorCodes(m_SharedInfo.m_DBConn);
		LOG.info("Loaded SysCDCErrorCodes.");
		LOG.debug("\n" + m_SharedInfo.m_ErrorCodes.toString());

		m_SharedInfo.m_RecTypes.loadSysCDCRecTypes(m_SharedInfo.m_DBConn);
		LOG.info("Loaded SysCDCRecTypes.");
		LOG.debug("\n" + m_SharedInfo.m_RecTypes.toString());

	}

	/**
	 * End CDC capture
	 * 
	 * @param sessionId
	 * @param mbz
	 * @param dbDesc
	 * @param userData
	 * @return return value from the CallableStatement
	 * @throws SQLException
	 */
	public int endcapture(int sessionId, int mbz, String dbDesc, int userData) throws SQLException {

		int retval = -1;
		CallableStatement cs = null;

		LOG.info(String.format("Executing endcapture with parameters: sessionId = %d, mbz = %d, dbDesc = %s", sessionId,
				mbz, dbDesc));

		try {
			cs = m_SharedInfo.m_DBConn.prepareCall("{ ? = call informix.cdc_endcapture(?, ?, ?) }");
			validateCallableStatement(cs);

			cs.setInt(1, sessionId);
			cs.setInt(2, mbz);
			cs.setString(3, dbDesc);

			ResultSet rs = cs.executeQuery();
			if (rs != null) {
				rs.next();
				retval = rs.getInt(1);
			}
		} catch (SQLException ex) {
			LOG.error("SQLerror", ex);
			ex.printStackTrace();
			throw ex;
		} finally {
			if (cs != null) {
				cs.close();
			}
		}
		return retval;
	}

	/**
	 * Convert an error code to a string from the table returned from the
	 * Informix server
	 * 
	 * @param errCode
	 * @return String describing the CDC error code
	 */
	public String errortext(int errCode) { // different from the CDC API
											// errortext function...
		SysCDCErrorCodesRecord erec;
		erec = m_SharedInfo.m_ErrorCodes.getSysCDCErrorCodesByKey(errCode);
		if (erec == null) {
			erec = new SysCDCErrorCodesRecord(errCode, "Unknown Error Code", "Unknown Error");
		}
		return erec.toString();
	}

	/**
	 * Call the API open session function
	 * 
	 * @param serverName
	 * @param sessionId
	 * @param timeoutInSecs
	 * @param maxRecs
	 * @param majorVersion
	 * @param minorVersion
	 * @return return value from the CallableStatement
	 * @throws SQLException
	 */
	public int opensess(String serverName, int sessionId, int timeoutInSecs, int maxRecs, int majorVersion,
			int minorVersion) throws SQLException {

		int retval = -1;
		CallableStatement cs = null;

		LOG.info(String.format(
				"Executing opensess with parameters: serverName = %s, sessionId = %d, timeout = %d, maxRecs = %d, majorVersion = %d, minorVersion = %d",
				serverName, sessionId, timeoutInSecs, maxRecs, majorVersion, minorVersion));

		try {
			cs = m_SharedInfo.m_DBConn.prepareCall("{ ? = call informix.cdc_opensess(?, ?, ?, ?, ?, ?) }");
			validateCallableStatement(cs);

			cs.setString(1, serverName);
			cs.setInt(2, sessionId);
			cs.setInt(3, timeoutInSecs);
			cs.setInt(4, maxRecs);
			cs.setInt(5, majorVersion);
			cs.setInt(6, minorVersion);

			ResultSet rs = cs.executeQuery();
			if (rs != null) {
				rs.next();
				retval = rs.getInt(1);
			}
			m_SharedInfo.m_SessionId = retval;
		} catch (SQLException ex) {
			LOG.error("SQLerror", ex);
			ex.printStackTrace();
			throw ex;
		} finally {
			if (cs != null) {
				cs.close();
			}
		}
		return retval;
	}

	/**
	 * Call the recboundary API function
	 * 
	 * @param sessionId
	 * @return return value from the CallableStatement
	 * @throws SQLException
	 */
	public int recboundary(int sessionId) throws SQLException {

		int retval = -1;
		CallableStatement cs = null;

		LOG.info(String.format("Executing recboundary with parameters: sessionId = %d", sessionId));

		try {
			cs = m_SharedInfo.m_DBConn.prepareCall("{ ? = call informix.cdc_recboundary(?) }");
			validateCallableStatement(cs);

			cs.setInt(1, sessionId);

			ResultSet rs = cs.executeQuery();
			if (rs != null) {
				rs.next();
				retval = rs.getInt(1);
			}
		} catch (SQLException ex) {
			LOG.error("SQLerror", ex);
			ex.printStackTrace();
			throw ex;
		} finally {
			if (cs != null) {
				cs.close();
			}
		}
		return retval;
	}

	/**
	 * Set full row logging - this must be set to do CDC
	 * 
	 * @param dbDesc
	 * @param logging
	 * @return return value from the CallableStatement
	 * @throws SQLException
	 */
	public int setfullrowlogging(String dbDesc, int logging) throws SQLException {

		int retval = -1;
		CallableStatement cs = null;

		LOG.info(String.format("Executing setfullrowlogging with parameters: dbDesc = %s, logging = %d", dbDesc,
				logging));

		try {
			cs = m_SharedInfo.m_DBConn.prepareCall("{ ? = call informix.cdc_set_fullrowlogging(?, ?) }");
			validateCallableStatement(cs);

			cs.setString(1, dbDesc);
			cs.setInt(2, logging);

			ResultSet rs = cs.executeQuery();
			if (rs != null) {
				rs.next();
				retval = rs.getInt(1);
			}
		} catch (SQLException ex) {
			LOG.error("SQLerror", ex);
			ex.printStackTrace();
			throw ex;
		} finally {
			if (cs != null) {
				cs.close();
			}
		}
		return retval;
	}

	/**
	 * Call the startcapture API function
	 * 
	 * @param sessionId
	 * @param mbz - Must Be Zero
	 * @param dbDesc
	 * @param colName
	 * @param userData
	 * @return return value from the CallableStatement
	 * @throws SQLException
	 */
	public int startcapture(int sessionId, int mbz, String dbDesc, String colName, int userData) throws SQLException {

		int retval = -1;
		CallableStatement cs = null;

		LOG.info(String.format(
				"Executing startcapture with parameters: sessionId = %d, mbz = %d, dbDesc = %s, colName = %s, userData = %d",
				sessionId, mbz, dbDesc, colName, userData));

		try {
			cs = m_SharedInfo.m_DBConn.prepareCall("{ ? = call informix.cdc_startcapture(?, ?, ?, ?, ?) }");
			validateCallableStatement(cs);

			cs.setInt(1, sessionId);
			cs.setInt(2, mbz);
			cs.setString(3, dbDesc);
			cs.setString(4, colName);
			cs.setInt(5, userData);

			ResultSet rs = cs.executeQuery();
			if (rs != null) {
				rs.next();
				retval = rs.getInt(1);
			}
		} catch (SQLException ex) {
			LOG.error("SQLerror", ex);
			ex.printStackTrace();
			throw ex;
		} finally {
			if (cs != null) {
				cs.close();
			}
		}
		return retval;
	}

	/**
	 * Verify the contents of a callable statement - this is the returned value
	 * of a JDBC call
	 * 
	 * @param cs
	 * @return TRUE if the CallableStatement is valid, FALSE otherwise
	 * @throws SQLException
	 */
	public boolean validateCallableStatement(CallableStatement cs) throws SQLException {

		int i = 0;
		java.sql.SQLWarning w = cs.getWarnings();
		while (w != null) {
			System.out.println("  Warning #" + ++i + ":");
			System.out.println("    SQLState = " + w.getSQLState());
			System.out.println("    Message  = " + w.getMessage());
			System.out.println("    SQLCODE  = " + w.getErrorCode());
			w = w.getNextWarning();
		}
		if (i > 0) {
			return false;
		}
		return true;
	}

	/**
	 * 
	 * @author pyoung class ColumnDef
	 */
	class ColumnDef {
		boolean isFixed;
		boolean isString;
		String name;
		String dataType;
		IfxType ifxDataType;
		int size; // size in bytes of the retrieved field (if fixed)
		int index;
	}

	class TableState {
		String tableToMonitor = "";
		String columnsToMonitor = "";
		String columnSizes = "";
		int tableIdentifier = -1;
		CDC_Table_Schema cdcTableSchema = new CDC_Table_Schema();
		boolean captureStarted = false;
		boolean loggingEnabled = false;

	}

	/**
	 * 
	 * @author pyoung
	 *
	 */
	class CDC_Table_Schema {
		int userData;
		int flags;
		int numFixedBytes;
		int numFixedCols;
		int numVarCols;

		ArrayList<ColumnDef> columns = new ArrayList<ColumnDef>();

	}

	/**
	 * 
	 * @author pyoung enum IfxType -
	 */
	enum IfxType {
		BIGINT, BYTE, CHARACTER, DATETIME, DATE, DECIMAL, DOUBLE, FLOAT, INT8, INTEGER, INTERVAL, MONEY, NUMERIC, REAL, SERIAL, SMALLFLOAT, SMALLINT, TEXT, VARCHAR, UNKNOWN
	}

	/**
	 * 
	 * @author pyoung
	 *
	 */
	class RecTypes {
		public static final int CDC_REC_BEGINTX = 1;
		public static final int CDC_REC_COMMTX = 2;
		public static final int CDC_REC_RBTX = 3;
		public static final int CDC_REC_INSERT = 40;
		public static final int CDC_REC_DELETE = 41;
		public static final int CDC_REC_UPDBEF = 42;
		public static final int CDC_REC_UPDAFT = 43;
		public static final int CDC_REC_TRUNCATE = 119;
		public static final int CDC_REC_TABSCHEMA = 200;
		public static final int CDC_REC_TIMEOUT = 201;
		public static final int CDC_REC_ERROR = 202;
	}

}
