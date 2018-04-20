/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vio.repository;

import com.vio.db.config.ConnectionPool;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author arito
 */
public class CRUDReflection implements CRUD<Object, Object>{

    /**
     * See java-jdbc-demo for the class
     */
    private final ConnectionPool CONNECTION_POOL = ConnectionPool.getInstance();
    
    
    @Override
    public Set<Object> findAll() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object findById(Object objectId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object update(Object object) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * 
     * @param object
     * @return 
     */
    @Override
    public Object save(Object object) {
        
        
        Class<?> clazz = null;
        Field[] clazz_fields_ = null;
        Map<String, Object> clazz_v = new HashMap<>();
        StringBuffer db_table_columns_buffer = new StringBuffer();
        StringBuffer object_values = new StringBuffer();
        try{
            clazz = object.getClass();
            clazz_fields_ = new Field[clazz.getDeclaredFields().length];
            clazz_fields_ = clazz.getDeclaredFields();
        } catch (Exception e){
            e.printStackTrace();
        }
        
        Object[] b = new Object[clazz_fields_.length];
        int i = 0;
        if(clazz_fields_.length > 0){
            for(Field f : clazz_fields_){
                f.setAccessible(true);
                clazz_v.put(f.getName(), f.getType());
                try{
                    b[i] = f.get(object);
                    i++;
                } catch (IllegalAccessException e){
                    e.printStackTrace();
                }
                
            }
            // the table name that matches the object class
            String DB_TABLE_NAME = null;
            
            // the DB_TABLE_NAME's columns
            String[] DB_TABLE_COLUMNS = null;
            
            DatabaseMetaData db = null;
            PreparedStatement statement = null;
            Connection connection = null;
            ResultSet result = null;
            String[] _table = null;
           
            try{
                
                String[] t_c = {"TABLE"};
                int p = 0;
                connection = CONNECTION_POOL.getConnection();
                db = connection.getMetaData();
                result = db.getTables(null, null, "%", t_c);
                
                _table = new String[10];
                while(result.next()){
                     
                     // add the table names into the array
                    _table[p] = result.getString(3);
                    // increment the _table index
                    p++;
                }
                
               
                
            } catch (SQLException ex) {
                Logger.getLogger(CRUDReflection.class.getName()).log(Level.SEVERE, null, ex);
            }
            
            
             // matching the table with the object's class name
             for(String s : _table){
                    if(object.getClass().getSimpleName().toLowerCase().contains(s.toLowerCase())){
                        DB_TABLE_NAME = s;
                        break;
                    }
                }
            
            try{
                final String QUERY = "SELECT * FROM " + DB_TABLE_NAME;
           
                statement = connection.prepareStatement(QUERY);
                result = statement.executeQuery();
                ResultSetMetaData rsmd_ = result.getMetaData();
                int columns_size = rsmd_.getColumnCount();
                
                DB_TABLE_COLUMNS = new String[columns_size - 1];
                String prefix_table = "";
                
                for(int k = 2; k <= columns_size; k++){
                   
                    DB_TABLE_COLUMNS[k-2] = rsmd_.getColumnName(k);
                    db_table_columns_buffer.append(prefix_table);
                    prefix_table = ",";
                    db_table_columns_buffer.append(rsmd_.getColumnName(k));
                   
                    
                }
                 
            } catch (SQLException ex) {
                Logger.getLogger(CRUDReflection.class.getName()).log(Level.SEVERE, null, ex);
            }
            
            final StringBuffer buff = new StringBuffer();
            String prefix = "";
            for(int j = 0; j < DB_TABLE_COLUMNS.length; j++){
                buff.append(prefix);
                prefix = ",";
                buff.append("?");
            }
            
            // insert query
            final String INSERT_QUERY = "INSERT INTO " + DB_TABLE_NAME + "(" + db_table_columns_buffer+ ")" + " VALUES(" + buff+ ")";
            System.out.println(b.length);
            try{
                statement = connection.prepareStatement(INSERT_QUERY);
                int m = 1;
                
                for(Object val : b){
                    System.out.println(val.toString());
                    if(val instanceof String){
                        statement.setString(m, val.toString());
                        m++;
                        continue;
                    }
                    if(val instanceof Integer){
                         statement.setInt(m, Integer.parseInt(val.toString()));
                        m++;
                        continue;
                    }
                    if(val instanceof Boolean){
                         statement.setBoolean(m, ((Boolean) val).booleanValue());
                        m++;
                        continue;
                    }
                    if(val instanceof Double){
                         statement.setDouble(m, ((Double) val).doubleValue());
                        m++;
                        continue;
                    }
                    if(val instanceof Float){
                         statement.setFloat(m, ((Float) val).floatValue());
                        m++;
                        continue;
                    }
                    
                }
                
                statement.executeUpdate();
                
            } catch (SQLException ex) {
                Logger.getLogger(CRUDReflection.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                CONNECTION_POOL.closeConnection(connection);
                try {
                    statement.close();
                    result.close();
                } catch (SQLException ex) {
                    Logger.getLogger(CRUDReflection.class.getName()).log(Level.SEVERE, null, ex);
                } 
            }
            
        }
        
        
        return object;
    }

    @Override
    public void delete(Object objectId) {
        
        final String CLASS_NAME = com.vio.domain.Product.class.getSimpleName();
        //final String CLASS_NAME = this.getClass().getSimpleName();
        String table_name_ = null;
        
        // get the table name
        DatabaseMetaData db = null;
        PreparedStatement statement = null;
        Connection connection = null;
        ResultSet result = null;
        String[] _table = null;
            
        try{
                
                String[] t_c = {"TABLE"};
                int p = 0;
                connection = CONNECTION_POOL.getConnection();
                db = connection.getMetaData();
                result = db.getTables(null, null, "%", t_c);
                
                _table = new String[10];
                while(result.next()){
                     
                     // add the table names into the array
                    _table[p] = result.getString(3);
                    // increment the _table index
                    p++;
                }
            } catch (SQLException ex) {
                Logger.getLogger(CRUDReflection.class.getName()).log(Level.SEVERE, null, ex);
            }
            for(String s : _table){
                if(CLASS_NAME.toLowerCase().contains(s.toLowerCase())){
                    table_name_ = s;
                    break;
                }
            }
            // get the ID column name just in case It's not default to ID
            String TABLE_ID_NAME = null;
            try{
               final String SELECT_QUERY = "SELECT * FROM " + table_name_;
               statement = connection.prepareStatement(SELECT_QUERY);
               result = statement.executeQuery();
               ResultSetMetaData rsmd = result.getMetaData();
               for(int i = 1; i < 2; i++){
                   System.out.println(rsmd.getColumnName(i) + " COL NAME");
                   TABLE_ID_NAME = rsmd.getColumnName(i);
               }
               
            } catch (SQLException ex) {
            Logger.getLogger(CRUDReflection.class.getName()).log(Level.SEVERE, null, ex);
        }
        
         
            final String DELETE_QUERY = "DELETE FROM " + table_name_ + " WHERE " + TABLE_ID_NAME + "=" + getType(objectId);
           
            try{
                statement = connection.prepareStatement(DELETE_QUERY);
                statement.executeUpdate();
            } catch (SQLException ex) {
            Logger.getLogger(CRUDReflection.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
                CONNECTION_POOL.closeConnection(connection);
            try {
                statement.close();
            } catch (SQLException ex) {
                Logger.getLogger(CRUDReflection.class.getName()).log(Level.SEVERE, null, ex);
            }
            }
    }
   
    private static Object getType(Object object){
        
        if(object instanceof String){
            return (String) object.toString();
        }
        if(object instanceof Integer){
            return (Integer) ((Integer) object).intValue();
        }
        if(object instanceof Double){
            return (Double) ((Double) object).doubleValue();
        }
        if(object instanceof Float){
            return (Float) ((Float) object).floatValue();
        }
        if(object instanceof Long){
            return (Long) ((Long) object).longValue();
        }
        
        return null;
    }
    
}
