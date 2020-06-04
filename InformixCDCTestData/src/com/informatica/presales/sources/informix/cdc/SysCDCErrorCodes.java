package com.informatica.presales.sources.informix.cdc;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;

class SysCDCErrorCodesRecord {
    public int errcode;
    public String errname;
    public String errdesc;
    
    public SysCDCErrorCodesRecord(int errcode, String errname, String errdesc) {
       this.errcode = errcode;
       this.errname = errname;
       this.errdesc = errdesc;
    }
    
    public String toString() {
       return String.format(
             "errcode = %d, errname = %s, errdesc = %s", errcode, errname, errdesc);
    }
 }

public class SysCDCErrorCodes {

   
   
   protected final static String SYSCDCERRCODES_SQL = 
           " SELECT errcode, errname, errdesc"
         + "  FROM syscdcerrcodes"
         + " FOR READ ONLY";
   
   protected HashMap<Integer, SysCDCErrorCodesRecord> mRecords = new HashMap<Integer, SysCDCErrorCodesRecord>();
   
   public SysCDCErrorCodes() {
   }
   
   public SysCDCErrorCodesRecord getSysCDCErrorCodesByKey(int errcode) {
      return mRecords.get(errcode);
   }
   
   public void loadSysCDCErrorCodes(Connection dbConn) throws SQLException {
      
      PreparedStatement ps = null;
      mRecords.clear();
      
      try {
         ps = dbConn.prepareStatement(SYSCDCERRCODES_SQL);
         ResultSet rs = ps.executeQuery();
         if (rs != null) {
            while (rs.next()) {
               mRecords.put(
                     rs.getInt(1),
                     new SysCDCErrorCodesRecord(rs.getInt(1), rs.getString(2), rs.getString(3)));
            }
            rs.close();
         }
         
      }
      catch (SQLException ex) {
         ex.printStackTrace();
         throw ex;
      }
      finally {
         if (ps != null) {
            ps.close();
         }
      }
      
   }
   
   public String toString() {
   
      StringBuffer sb = new StringBuffer();
      for (Iterator<SysCDCErrorCodesRecord> i = mRecords.values().iterator(); i.hasNext();) {
         sb.append(i.next().toString()).append("\n");
      }
      return sb.toString();
   }
}
