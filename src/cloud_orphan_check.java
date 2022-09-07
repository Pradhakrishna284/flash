import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class cloud_orphan_check {

	public static void main(String[] args) throws SQLException, ClassNotFoundException {

			// TDR database(Determination)
			String driver = "oracle.jdbc.driver.OracleDriver";
			String jdbc_url = "jdbc:oracle:thin:@content.qa.db.int.thomsonreuters.com:1521:ndd0148b";
			String username_TDR = "CONTENT_REPO ";
			String password_TDR = "RedWhale";

			Connection dbconnect = null;
			Statement stmt = null;
			ResultSet resultSet = null;		

			try 
			{
				// create jdbc connection object and load class
				Class.forName(driver);
				dbconnect = DriverManager.getConnection(jdbc_url, username_TDR, password_TDR);
				dbconnect.setSchema("SBXTAX5");

				if (dbconnect != null) 
				{
					System.out.println("CONNECTED TO SBXTAX5 SCHEMA");

					ArrayList<String> arrEntities = GetEntities();

					int counter = 0;
					for(String entity : arrEntities) 
					{
						// Create statement object
						stmt = dbconnect.createStatement();						
						stmt.setFetchSize(20000);

						counter = counter + 1;
						String queryToExecute = GenerateQuery(counter, entity);

						PreparedStatement prepared_statement = dbconnect.prepareStatement(queryToExecute,
								ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
						
						resultSet = prepared_statement.executeQuery();

						// Retrieving the ResultSetMetadata object
						ResultSetMetaData rsMetaData = resultSet.getMetaData();
						// Retrieving the list of column names
						int count = rsMetaData.getColumnCount();

						int rowCount = 0;

						// If rs.last() returns false that means size of ResultSet object is 0
						if (resultSet.last()) {
							// make cursor to point to the last row in the ResultSet object
							rowCount = resultSet.getRow();
							resultSet.beforeFirst();

							// print column names
							for (int j = 1; j <= count; j++) {
								System.out.print(rsMetaData.getColumnName(j) + "\t\t");
							}
							System.out.println(" ");
							while (resultSet.next()) {
								for (int j = 1; j <= count; j++) {
									if (j != count)
										// System.out.print(resultSet.getString(j) + "\t" + " --> ");
										System.out.print(resultSet.getString(j) + "\t\t");
									else
										System.out.println(resultSet.getString(j));
								}
							}
						}
						else {
							System.out.println("There are NO duplicate UUIDs");
						}
						System.out.println(" ");

						if(stmt !=null)
							stmt.close();
					}
				} else {
					System.out.println("Not connected to database");
				}
			} catch (SQLException e) {
				System.out.println(e.getErrorCode());
				System.out.println(e.getMessage());
				System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
				System.out.println("There is an execption, I am in catch block");
			} finally {
//				if (resultSet != null)
//					resultSet.close();
//				dbconnect.close();
				try {
				      if (null != stmt)
				        stmt.close();
				    } catch (Exception e) {
				    	System.out.println();
				    }
				    try {
				      if (null != dbconnect)
				    	  dbconnect.close();
				    } catch (Throwable e) {/* do nothing */
				    }
			}
		}

		public static ArrayList<String> GetEntities()
		{
			ArrayList<String> arrayEntitiesList = new ArrayList<String>();	
			arrayEntitiesList.add("'ZoneAuthorities'");		
			arrayEntitiesList.add("'Zones'");		
			arrayEntitiesList.add("'UniqueAreaAuthorities'");
			arrayEntitiesList.add("'UniqueAreas'");
			arrayEntitiesList.add("'ComplianceAreaAuthorities'");

			return arrayEntitiesList;
		}

		public static String GenerateQuery(int index, String entity) {

			String buildQuery = "";
			System.out.println(index + ") " + entity);

			switch (entity) {
			case "'ZoneAuthorities'":
				buildQuery =  "SELECT o.*, ta.uuid, ta.NAME authority_name "
						+ " FROM ( "
						+ " SELECT za.zone_authority_id ,za.zone_id ,za.authority_id "
						+ " FROM  tb_zone_authorities za "
						+ " WHERE ( "
						+ " NOT EXISTS ( "
						+ " SELECT 1 "
						+ " FROM  tb_authorities au "
						+ " WHERE au.authority_id = za.authority_id "
						+ " ) "
						+ " OR NOT EXISTS ( "
						+ " SELECT 1 "
						+ " FROM  tb_zones z "
						+ " WHERE z.zone_id = za.zone_id "
						+ " ) "
						+ " ) "
						+ " ) o "
						+ " LEFT JOIN tb_authorities ta ON o.authority_id = ta.authority_id "
						+ " JOIN tb_merchants m ON ta.merchant_id = m.merchant_id "
						+ " WHERE m.name = 'Sabrix US Tax Data' "
						+ " ORDER BY 2,1,3 ";
				break;

				case "'Zones'":
				buildQuery =  "SELECT tz.*\r\n"
						+ " FROM  tb_zones tz\r\n"
						+ " JOIN tb_merchants m ON tz.merchant_id = m.merchant_id\r\n"
						+ " WHERE tz.name != 'WORLD'\r\n"
						+ " AND tz.name != 'ZONE_ID placeholder'\r\n"
						+ " AND NOT EXISTS (\r\n"
						+ " SELECT 1\r\n"
						+ " FROM  tb_zones z\r\n"
						+ " WHERE z.zone_id = tz.parent_zone_id\r\n"
						+ " AND z.merchant_id = tz.merchant_id\r\n"
						+ " )\r\n"
						+ " AND m.name = 'Sabrix US Tax Data'\r\n"
						+ "ORDER BY 2,3";
				break;

			case "'UniqueAreaAuthorities'":
				buildQuery =  "SELECT o.*, ta.uuid, ta.NAME authority_name\r\n"
						+ " FROM (\r\n"
						+ " SELECT uaa.unique_area_authority_id ,uaa.unique_area_authority_uuid\r\n"
						+ " ,uaa.unique_area_id, uaa.authority_id\r\n"
						+ " FROM  tb_unique_area_authorities uaa\r\n"
						+ " WHERE (\r\n"
						+ " NOT EXISTS (\r\n"
						+ " SELECT 1\r\n"
						+ " FROM  tb_authorities au\r\n"
						+ " WHERE au.authority_id = uaa.authority_id\r\n"
						+ " )\r\n"
						+ " OR NOT EXISTS (\r\n"
						+ " SELECT 1\r\n"
						+ " FROM  tb_unique_areas ua\r\n"
						+ " WHERE ua.unique_area_id = uaa.unique_area_id\r\n"
						+ " )\r\n"
						+ " )\r\n"
						+ " ) o\r\n"
						+ " LEFT JOIN tb_authorities ta ON o.authority_id = ta.authority_id\r\n"
						+ " JOIN tb_merchants m ON ta.merchant_id = m.merchant_id\r\n"
						+ " WHERE m.name = 'Sabrix US Tax Data'\r\n"
						+ " ORDER BY 2,1,3";	
				break;

			case "'UniqueAreas'":
				//Note:- I replaced ua.* with all the column names except area_polygon since it has xml data
				buildQuery = "SELECT ua.unique_area_id, ua.unique_area_uuid, ua.uaid, ua.area_zone, ua.compliance_area_id, ua.merchant_id,\r\n"
						+ "ua.merchant_uuid, ua.start_date, ua.end_date, ua.created_by, ua.creation_date, ua.last_updated_by, \r\n"
						+ "ua.last_update_date, ua.synchronization_timestamp, ua.uuid, ua.compliance_area_content_uuid, \r\n"
						+ "ua.compliance_area_uuid \r\n"
						+ " FROM  tb_unique_areas ua\r\n"
						+ " JOIN tb_merchants m ON ua.merchant_id = m.merchant_id\r\n"
						+ " WHERE NOT EXISTS (\r\n"
						+ " SELECT 1\r\n"
						+ " FROM  tb_compliance_areas ca\r\n"
						+ " WHERE ca.compliance_area_id = ua.compliance_area_id\r\n"
						+ " AND ca.merchant_id = ua.merchant_id\r\n"
						+ " )\r\n"
						+ " AND m.name = 'Sabrix US Tax Data'\r\n"
						+ "ORDER BY 3";	
				break;
				
			case "'ComplianceAreaAuthorities'":
				buildQuery = "SELECT o.*, ta.uuid, ta.NAME authority_name\r\n"
						+ " FROM (\r\n"
						+ " SELECT caa.compliance_area_auth_id ,caa.compliance_area_id ,caa.authority_id\r\n"
						+ " FROM  tb_comp_area_authorities caa\r\n"
						+ " WHERE (\r\n"
						+ " NOT EXISTS (\r\n"
						+ " SELECT 1\r\n"
						+ " FROM  tb_authorities au\r\n"
						+ " WHERE au.authority_id = caa.authority_id\r\n"
						+ " )\r\n"
						+ " OR NOT EXISTS (\r\n"
						+ " SELECT 1\r\n"
						+ " FROM  tb_compliance_areas ca\r\n"
						+ " WHERE ca.compliance_area_id = caa.compliance_area_id\r\n"
						+ " )\r\n"
						+ " )\r\n"
						+ " ) o\r\n"
						+ " LEFT JOIN tb_authorities ta ON o.authority_id = ta.authority_id\r\n"
						+ " JOIN tb_merchants m ON ta.merchant_id = m.merchant_id\r\n"
						+ " WHERE m.name = 'Sabrix US Tax Data'\r\n"
						+ " ORDER BY 2,1,3";	
				break;

			default:
				System.out.println("Default query executed");
				buildQuery = "select * from TB_TRANSPORTATION_TYPES";
				break;
			}
			return buildQuery;
		}
	}
