package com.informatica.presales.sources.informix.cdc;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;

public class SysCDCRecTypes {

   class SysCDCRecTypesRecord {
      public int recnum;
      public String recname;
      public String recdesc;
      
      public SysCDCRecTypesRecord(int recnum, String recname, String recdesc) {
         this.recnum  = recnum;
         this.recname = recname;
         this.recdesc = recdesc;
      }
      
      public String toString() {
         return String.format(
               "recnum = %d, recname = %s, recdesc = %s", recnum, recname, recdesc);
      }
   }
   
   protected final static String SYSCDCRECTYPES_SQL = 
           " SELECT recnum, recname, recdesc"
         + "  FROM syscdcrectypes"
         + " WHERE recname LIKE 'CDC%'"
         + " FOR READ ONLY";
   
   protected HashMap<Integer, SysCDCRecTypesRecord> mRecords = new HashMap<Integer, SysCDCRecTypesRecord>();
   
   public SysCDCRecTypes() {
   }
   
   public SysCDCRecTypesRecord getSysCDCRecTypeByKey(int recnum) {
      return mRecords.get(recnum);
   }
   
   public void loadSysCDCRecTypes(Connection dbConn) throws SQLException {
      
      PreparedStatement ps = null;
      mRecords.clear();
      
      try {
         ps = dbConn.prepareStatement(SYSCDCRECTYPES_SQL);
         ResultSet rs = ps.executeQuery();
         if (rs != null) {
            while (rs.next()) {
               mRecords.put(
                     rs.getInt(1),
                     new SysCDCRecTypesRecord(rs.getInt(1), rs.getString(2), rs.getString(3)));
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
      for (Iterator<SysCDCRecTypesRecord> i = mRecords.values().iterator(); i.hasNext();) {
         sb.append(i.next().toString()).append("\n");
      }
      return sb.toString();
   }
}
