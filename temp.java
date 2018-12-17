import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {

	private String configFilename;
	private Properties configProps = new Properties();

	private String jSQLDriver;
	private String jSQLUrl;
	private String jSQLUser;
	private String jSQLPassword;

	// DB Connection
	private Connection conn;

	// Logged In User
	private String username; // Unique customer ID
        private String full_name; // Customer name

	// Canned queries
    private ArrayList<ArrayList<Integer>> itineraries;
    
    // search (one hop) -- This query assumes the month in question is July 2015
	private static final String SEARCH_ONE_HOP_SQL =
			"SELECT TOP (?) year,month_id,day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,fid "
					+ "FROM Flights "
					+ "WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? "
					+ "AND month_id = 7 AND year = 2015 "
					+ "AND actual_time IS NOT NULL "
					+ "ORDER BY actual_time ASC";
	private PreparedStatement searchOneHopStatement;
	
    // search (two hop) -- This query assumes the month in question is July 2015	
	private static final String SEARCH_TWO_HOP_SQL =
			"SELECT TOP (?) F.year,F.month_id,F.day_of_month,F.carrier_id,F.flight_num,F.origin_city,F.dest_city,F.actual_time,F.fid, "
					+ "S.year,S.month_id,S.day_of_month,S.carrier_id,S.flight_num,S.origin_city,S.dest_city,S.actual_time,S.fid, "
					+ "(F.actual_time + S.actual_time) AS total_time "
					+ "FROM Flights AS F "
					+ "INNER JOIN Flights AS S "
					+ "ON S.origin_city = F.dest_city "
					+ "AND F.origin_city = ? "
					+ "AND S.dest_city = ? "
					+ "AND F.day_of_month = ? "
					+ "AND S.day_of_month > F.day_of_month "
					+ "WHERE F.actual_time IS NOT NULL "
					+ "AND S.actual_time IS NOT NULL "
					+ "AND F.month_id = 7 AND F.year = 2015 "
					+ "AND S.month_id = 7 AND S.year = 2015 "
					+ "ORDER BY S.day_of_month ASC, F.actual_time + S.actual_time ASC";
	private PreparedStatement searchTwoHopStatement;
	
	// login -- This query grabs the user id from the database for the given username and password
	private static final String LOGIN_SQL =
			"SELECT full_name "
					+ "FROM Customer "
					+ "WHERE user_name = ? AND password = ?";
	private PreparedStatement loginStatement;
	
	private static final String CHECK_FLIGHT_SQL =
			"SELECT passengers_count "
					+ "FROM Booking "
					+ "WHERE flight_id = ?";
	private PreparedStatement checkFlightStatement;
	
	private static final String NEW_BOOKING_SQL =
			"INSERT INTO Booking "
					+ "VALUES(?, 1)";
	private PreparedStatement newBookingStatement;
	
	private static final String UPDATE_BOOKING_SQL =
			"UPDATE Booking "
					+ "SET passengers_count = ? "
					+ "WHERE flight_id = ?";
	private PreparedStatement updateBookingStatement;
	
	private static final String CHECK_BOOKING_SQL =
			"SELECT passengers_count "
					+ "FROM Booking "
					+ "WHERE flight_id = ?";
	private PreparedStatement checkBookingStatement;
	
	private static final String RESERVATION_SQL =
			"INSERT INTO Reservation "
					+ "VALUES(?, ?, ?, ?)";
	private PreparedStatement reserveStatement;
	
	private static final String LIST_RESERVATIONS_SQL =
			"SELECT reservation_id, flight_id "
					+ "FROM Reservation "
					+ "WHERE user_name = ? "
					+ "ORDER BY date ASC";
	private PreparedStatement listStatement;
	
	private static final String FIND_FLIGHT_SQL =
			"SELECT year,month_id,day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,fid "
					+ "FROM Flights "
					+ "WHERE fid = ? "
					+ "ORDER BY day_of_month ASC";
	private PreparedStatement flightStatement;
	
	private static final String FIND_RESERVATION_SQL =
			"SELECT flight_id "
					+ "FROM Reservation "
					+ "WHERE user_name = ? "
					+ "AND reservation_id = ?";
	private PreparedStatement findStatement;
	
	private static final String CANCEL_SQL =
			"DELETE FROM Reservation "
					+ "WHERE user_name = ? "
					+ "AND reservation_id = ?";
	private PreparedStatement cancelStatement;
	
	// transactions
	private static final String BEGIN_TRANSACTION_SQL =  
			"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;"; 
	private PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT TRANSACTION";
	private PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
	private PreparedStatement rollbackTransactionStatement;


	public Query(String configFilename) {
		this.configFilename = configFilename;
	}

	/**********************************************************/
	/* Connection code to SQL Azure.  */
	public void openConnection() throws Exception {
		configProps.load(new FileInputStream(configFilename));

		jSQLDriver   = configProps.getProperty("flightservice.jdbc_driver");
		jSQLUrl	   = configProps.getProperty("flightservice.url");
		jSQLUser	   = configProps.getProperty("flightservice.sqlazure_username");
		jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

		/* load jdbc drivers */
		Class.forName(jSQLDriver).newInstance();

		/* open connections to the flights database */
		conn = DriverManager.getConnection(jSQLUrl, // database
				jSQLUser, // user
				jSQLPassword); // password

		conn.setAutoCommit(true); //by default automatically commit after each statement 

		/* You will also want to appropriately set the 
                   transaction's isolation level through:  
		   conn.setTransactionIsolation(...) */

	}

	public void closeConnection() throws Exception {
		conn.close();
	}

	/**********************************************************/
	/* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

	public void prepareStatements() throws Exception {
		searchOneHopStatement = conn.prepareStatement(SEARCH_ONE_HOP_SQL);
		searchTwoHopStatement = conn.prepareStatement(SEARCH_TWO_HOP_SQL);		
		loginStatement = conn.prepareStatement(LOGIN_SQL);
 		reserveStatement = conn.prepareStatement(RESERVATION_SQL);
 		checkFlightStatement = conn.prepareStatement(CHECK_FLIGHT_SQL);
 		newBookingStatement = conn.prepareStatement(NEW_BOOKING_SQL);
 		updateBookingStatement = conn.prepareStatement(UPDATE_BOOKING_SQL);
 		checkBookingStatement = conn.prepareStatement(CHECK_BOOKING_SQL);
 		listStatement = conn.prepareStatement(LIST_RESERVATIONS_SQL);
 		flightStatement = conn.prepareStatement(FIND_FLIGHT_SQL);
 		findStatement = conn.prepareStatement(FIND_RESERVATION_SQL);
        cancelStatement = conn.prepareStatement(CANCEL_SQL);
         

 		beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);
		itineraries = new ArrayList<ArrayList<Integer>>();
	}
	
	public void transaction_login(String username, String password) throws Exception {
		loginStatement.clearParameters();
		loginStatement.setString(1, username);
		loginStatement.setString(2, password);
		ResultSet loginResults = loginStatement.executeQuery();
		if (loginResults.next()) {
			this.full_name = loginResults.getString("full_name");
			this.username = username;
			System.out.println("Logged in as " + full_name + ".");
		} else {
			System.out.println("Incorrect username or password.");
		}
		loginResults.close();
	}

	/**
	 * Searches for flights from the given origin city to the given destination
	 * city, on the given day of the month. If "directFlight" is true, it only
	 * searches for direct flights, otherwise is searches for direct flights
	 * and flights with two "hops". Only searches for up to the number of
	 * itineraries given.
	 * Prints the results found by the search.
	 */
	public void transaction_search_safe(String originCity, String destinationCity, boolean directFlight,
										int dayOfMonth, int numberOfItineraries) throws Exception {
		
		System.out.println();
		itineraries.clear();
		
		// one hop itineraries
		searchOneHopStatement.clearParameters();
		searchOneHopStatement.setInt(1, numberOfItineraries);
		searchOneHopStatement.setString(2, originCity);
		searchOneHopStatement.setString(3, destinationCity);
		searchOneHopStatement.setInt(4, dayOfMonth);
		ResultSet oneHopResults = searchOneHopStatement.executeQuery();
		int row_id = 0;
		int flight_count = 0;
		while (oneHopResults.next() && flight_count < numberOfItineraries) {
					row_id++;
					flight_count++;
					System.out.println("Itinerary " + row_id + ", one flight, " + oneHopResults.getInt("actual_time") + " minutes.");
					String results = extract_results(oneHopResults, 1, true, true);
                    System.out.println("Flight: " + results);
                    System.out.println();
  		}
		oneHopResults.close();

		if (!directFlight && flight_count < numberOfItineraries) {
			// two hop itineraries
			searchTwoHopStatement.clearParameters();
			searchTwoHopStatement.setInt(1, numberOfItineraries);
			searchTwoHopStatement.setString(2, originCity);
			searchTwoHopStatement.setString(3, destinationCity);
			searchTwoHopStatement.setInt(4, dayOfMonth);
			ResultSet twoHopResults = searchTwoHopStatement.executeQuery();
			while (twoHopResults.next() && flight_count < numberOfItineraries) {
					row_id++;
					flight_count++;
					System.out.println("Itinerary " + row_id + ", two flights, " + twoHopResults.getInt("total_time") + " minutes.");					
					String results_first = extract_results(twoHopResults, 1, true, true);
					System.out.println("Flight One: " + results_first);
					String results_second = extract_results(twoHopResults, 2, true, false);
					System.out.println("Flight Two: " + results_second);
					System.out.println();
			}
			twoHopResults.close();
		}
	}


	private String extract_results(ResultSet data, int FlightNumber, boolean saveFlight, boolean firstFlight) throws Exception {
        int result_year = data.getInt((FlightNumber-1)*9 + 1);
        int result_monthId = data.getInt((FlightNumber-1)*9 + 2);
        int result_dayOfMonth = data.getInt((FlightNumber-1)*9 + 3);
        String result_carrierId = data.getString((FlightNumber-1)*9 + 4);
        String result_flightNum = data.getString((FlightNumber-1)*9 + 5);
        String result_originCity = data.getString((FlightNumber-1)*9 + 6);
        String result_destCity = data.getString((FlightNumber-1)*9 + 7);
        int result_time = data.getInt((FlightNumber-1)*9 + 8);
        int flight_id = data.getInt((FlightNumber-1)*9 + 9);
        if (saveFlight) {
        	if (firstFlight) {
        		itineraries.add(new ArrayList<Integer>());
        	}
	    	int last = (itineraries.size() - 1);
	    	itineraries.get(last).add(flight_id);
	    	itineraries.get(last).add(result_dayOfMonth);
        }
        return result_year + ", " + result_monthId + ", " + result_dayOfMonth + ", carrier " + result_carrierId + ", flight number " 
        		+ result_flightNum + ", " + result_originCity + " to " + result_destCity + ", " + result_time + " minutes";
	}
	
	public void transaction_book(int itineraryId) throws Exception {
		if (username == null) {
			System.out.println("Please login to book an itinerary.");
			return;
		} else if (itineraryId < 0 || itineraryId > itineraries.size()) {
			System.out.println("That itinerary does not exist.");
			return;
		}
		beginTransaction();
		ArrayList<Integer> itinerary = itineraries.get(itineraryId - 1);
		for (int flight = 1; flight*2 <= itinerary.size(); flight++) {
			int flight_id = itinerary.get(flight*2 - 2);
			int date = itinerary.get(flight*2 - 1);
			checkFlightStatement.clearParameters();
			checkFlightStatement.setInt(1, flight_id);
			ResultSet flightResults = checkFlightStatement.executeQuery();
			boolean hasBookings = flightResults.next();
			if (!hasBookings) {
				newBookingStatement.clearParameters();
				newBookingStatement.setInt(1, flight_id);
				newBookingStatement.executeUpdate();
				reserve_spot(username, flight_id, date);
			} else if(flightResults.getInt("passengers_count") < 3) {
				updateBookingStatement.clearParameters();
				updateBookingStatement.setInt(1, flightResults.getInt("passengers_count") + 1);
				updateBookingStatement.setInt(2, flight_id);
				updateBookingStatement.executeUpdate();
				reserve_spot(username, flight_id, date);
			} else {
				System.out.println("The flight " + flight_id + " is full and cannot be booked. Itinerary canceled.");
				rollbackTransaction();
				return;
			}
		}
		commitTransaction();
	}

	private void reserve_spot(String username, int fid, int date) throws Exception {
		int rid = (int) Math.ceil(Math.random() * 999999999);
		reserveStatement.clearParameters();
		reserveStatement.setString(1, username);
		reserveStatement.setInt(2, rid);
		reserveStatement.setInt(3, fid);
		reserveStatement.setInt(4, date);
		try {
			reserveStatement.executeUpdate();
		} catch (SQLException e) {
			if (isConstraintViolation(e)) {
				System.out.println("You are only allowed one reservation per day. Itinerary canceled.");
			} else {
				System.out.println("An error occured. Itinerary canceled. Try again.");
			}
			rollbackTransaction();
			return;
		}
		System.out.println("Reservation complete, ID number " + rid +".");
	}
	
	private static boolean isConstraintViolation(SQLException e) {
	    return e.getSQLState().startsWith("23");
	}

	public void transaction_reservations() throws Exception {
		if (username == null) {
			System.out.println("Please login to view reservations.");
			return;
		}
		listStatement.clearParameters();
		listStatement.setString(1, username);
		ResultSet reservations = listStatement.executeQuery();
		if (reservations.isBeforeFirst()) {
			while(reservations.next()) {
				int rid = reservations.getInt("reservation_id");
				int fid = reservations.getInt("flight_id");
				flightStatement.clearParameters();
				flightStatement.setInt(1, fid);
				ResultSet flight = flightStatement.executeQuery();
				flight.next();
	            System.out.println();
	            System.out.println("Reservation " + rid);
				System.out.println("Flight: " + extract_results(flight, 1, false, false));
			}
		} else {
			System.out.println("No reservations for " + full_name + ".");
		}
	}

	public void transaction_cancel(int reservationId) throws Exception {
		if (username == null) {
			System.out.println("Please login to cancel reservations.");
			return;
		}
		beginTransaction();
		findStatement.clearParameters();
		findStatement.setString(1, username);
		findStatement.setInt(2, reservationId);
		ResultSet find = findStatement.executeQuery();
		if (find.isBeforeFirst()) {
			find.next();
			cancelStatement.clearParameters();
			cancelStatement.setString(1, username);
			cancelStatement.setInt(2, reservationId);
			cancelStatement.executeUpdate();
			checkBookingStatement.clearParameters();
			checkBookingStatement.setInt(1, find.getInt("flight_id"));
			ResultSet flight = checkBookingStatement.executeQuery();
			flight.next();
			updateBookingStatement.clearParameters();
			updateBookingStatement.setInt(1, flight.getInt("passengers_count") - 1);
			updateBookingStatement.setInt(2, find.getInt("flight_id"));
			updateBookingStatement.executeUpdate();
			System.out.println("Reservation " + reservationId + " canceled successfully.");
			commitTransaction();
		} else {
			System.out.println("Couldn't find that reservation.");
			rollbackTransaction();
		}
	}

    
       public void beginTransaction() throws Exception {
            conn.setAutoCommit(false);
            beginTransactionStatement.executeUpdate();  
        }

        public void commitTransaction() throws Exception {
            commitTransactionStatement.executeUpdate(); 
            conn.setAutoCommit(true);
        }
        public void rollbackTransaction() throws Exception {
            rollbackTransactionStatement.executeUpdate();
            conn.setAutoCommit(true);
        } 

}