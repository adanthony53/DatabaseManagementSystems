import java.io.FileInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

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
    private String username; // customer username is unique

    // Canned queries

    private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
    private PreparedStatement checkFlightCapacityStatement;
	
	private static final String CHECK_RESERVATION_ROW_NUMBER = "SELECT COUNT(*) AS row_number FROM Reservation";
    private PreparedStatement checkReservationRowStatement;

    // transactions
    private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
    private PreparedStatement beginTransactionStatement;

    private static final String COMMIT_SQL = "COMMIT TRANSACTION";
    private PreparedStatement commitTransactionStatement;

    private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
    private PreparedStatement rollbackTransactionStatement;

	
	
	// EXTRA
	private ArrayList<ArrayList<Integer>> itineraries;
	
	private static final String LOGIN_SQL = "SELECT * FROM Users WHERE username = ? AND password = ?";
	private PreparedStatement userLoginStatement;
	
	private static final String CREATE_USER_SQL = "INSERT INTO Users VALUES (?, ?, ?)";
	private PreparedStatement createUserStatement;
	
	private static final String SEARCH_ONE_HOP_SQL = "SELECT TOP (?) fid, day_of_month, carrier_id, flight_num, " 
		+ "origin_city, dest_city, actual_time AS total_time, capacity, price FROM Flights " 
		+ "WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? "
		+ "AND actual_time IS NOT NULL " 
		+ "ORDER BY actual_time ASC";
	private PreparedStatement searchOneHopStatement;
	
	private static final String SEARCH_TWO_HOP_SQL = "SELECT TOP (?) "
		+ "F.fid AS fid1,F.day_of_month AS day_of_month1,F.carrier_id AS carrier_id1, F.capacity AS capacity1, F.price AS price1,"
		+ "F.flight_num AS flight_num1,F.origin_city AS origin_city1,F.dest_city AS dest_city1,F.actual_time AS actual_time1,"
		+ "S.fid AS fid2,S.day_of_month AS day_of_month2,S.carrier_id AS carrier_id2, S.capacity AS capacity2, S.price AS price2,"
		+ "S.flight_num AS flight_num2,S.origin_city AS origin_city2,S.dest_city AS dest_city2,S.actual_time AS actual_time2,"
		+ "(F.actual_time + S.actual_time) AS total_time "
		+ "FROM Flights AS F "
		+ "INNER JOIN Flights AS S "
		+ "ON S.origin_city = F.dest_city "
		+ "AND F.origin_city = ? "
		+ "AND S.dest_city = ? "
		+ "AND F.day_of_month = ? "
		+ "AND F.day_of_month = S.day_of_month "
		+ "WHERE F.actual_time IS NOT NULL "
		+ "AND S.actual_time IS NOT NULL "
		+ "ORDER BY (F.actual_time + S.actual_time) ASC";
	private PreparedStatement searchTwoHopStatement;
	
	private static final String CHECK_BOOKING_SQL = "SELECT * FROM Booking WHERE fid = ?";
	private PreparedStatement checkBookingStatement;
	
	private static final String NEW_BOOKING_SQL = "INSERT INTO Booking VALUES (?, 1)";
	private PreparedStatement newBookingStatement;
	
	private static final String UPDATE_BOOKING_SQL = "UPDATE Booking SET count = ? WHERE fid = ?";
	private PreparedStatement updateBookingStatement;
	
	private static final String ONE_RESERVATION_SQL = "INSERT INTO Reservation VALUES (?, ?, ?, NULL, ?, ?)";
	private PreparedStatement oneReservationStatement;
	
	private static final String TWO_RESERVATION_SQL = "INSERT INTO Reservation VALUES (?, ?, ?, ?, ?, ?)";
	private PreparedStatement twoReservationStatement;
	
	private static final String UPDATE_RESERVATION_SQL = "UPDATE Reservation SET fid2 = ? WHERE rid = ?";
	private PreparedStatement updateReservationStatement;
	
	private static final String SEARCH_RESERVATION_SQL = "SELECT * FROM Reservation WHERE username = ?";
	private PreparedStatement searchReservationStatement;
	
	private static final String SEARCH_UNPAID_SQL = "SELECT * FROM Reservation WHERE username = ? AND rid = ? AND paid = 'false'";
	private PreparedStatement unpaidReservationStatement;
	
	private static final String SEARCH_UNPAID_SQL_2 = "SELECT fid2 FROM Reservation WHERE username = ? AND rid = ? AND paid = 'false'";
	private PreparedStatement unpaidReservationStatement2;
	
	private static final String UPDATE_UNPAID_SQL = "UPDATE Reservation SET paid = ? WHERE rid = ?";
	private PreparedStatement updatePaidStatement;
	
	private static final String UPDATE_BALANCE_SQL = "UPDATE Users SET balance = ? WHERE username = ?";
	private PreparedStatement updateBalanceStatement;
	
	private static final String FLIGHT_PRICE_SQL = "SELECT price FROM Flights WHERE FID = ?";
	private PreparedStatement checkPriceStatement;
	
	private static final String SEARCH_FLIGHT_SQL = "SELECT * FROM Flights WHERE fid = ?";
	private PreparedStatement searchFlightStatement;
	
	private static final String SEARCH_BALANCE_SQL = "SELECT * FROM Users WHERE username = ?";
	private PreparedStatement checkBalanceStatement;
	
	private static final String SECOND_FID_SQL = "SELECT fid2 FROM Reservation WHERE username = ? AND rid = ?";
	private PreparedStatement searchSecondFidStatement;
	
	private static final String FIRST_FID_SQL = "SELECT fid1 FROM Reservation WHERE username = ? AND rid = ?";
	private PreparedStatement searchFirstFidStatement;

	private static final String CANCEL_RESERVATION_SQL = "DELETE FROM Reservation WHERE username = ? AND rid = ?";
	private PreparedStatement cancelReservationStatement;
	
	private static final String DELETE_RESERVATION_SQL = "DELETE FROM Reservation";
	private PreparedStatement deleteReservationStatement;
	
	private static final String DELETE_BOOKING_SQL = "DELETE FROM Booking";
	private PreparedStatement deleteBookingStatement;
	
	private static final String DELETE_USER_SQL = "DELETE FROM Users";
	private PreparedStatement deleteUserStatement;

	
    class Flight {
        public int fid;
        public int dayOfMonth;
        public String carrierId;
        public String flightNum;
        public String originCity;
        public String destCity;
        public int time;
        public int capacity;
        public int price;

        @Override
        public String toString() {
            return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
                " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
                " Capacity: " + capacity + " Price: " + price;
        }
    }

    public Query(String configFilename) {
        this.configFilename = configFilename;
    }

    /* 
		Connection code to SQL Azure.  
	*/
    public void openConnection() throws Exception {
        configProps.load(new FileInputStream(configFilename));

        jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
        jSQLUrl = configProps.getProperty("flightservice.url");
        jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
        jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

        /* load jdbc drivers */
        Class.forName(jSQLDriver).newInstance();

        /* open connections to the flights database */
        conn = DriverManager.getConnection(jSQLUrl, // database
            jSQLUser, // user
            jSQLPassword); // password

        conn.setAutoCommit(true); //by default automatically commit after each statement

        /* You will also want to appropriately set the transaction's isolation level through:
           conn.setTransactionIsolation(...)
           See Connection class' JavaDoc for details.
        */
    }

    public void closeConnection() throws Exception {
        conn.close();
    }

    /**
     * Clear the data in any custom tables created. Do not drop any tables and do not
     * clear the flights table. You should clear any tables you use to store reservations
     * and reset the next reservation ID to be 1.
     */
    public void clearTables() {
        // your code here
		try {
			deleteReservationStatement.executeUpdate();
			deleteBookingStatement.executeUpdate();
			deleteUserStatement.executeUpdate();
		} catch (SQLException e){
			e.printStackTrace();
		}
    }

    /**
     * prepare all the SQL statements in this method.
     * "preparing" a statement is almost like compiling it.
     * Note that the parameters (with ?) are still not filled in
     */
    public void prepareStatements() throws Exception {
        beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
        commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
        rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);

		deleteReservationStatement = conn.prepareStatement(DELETE_RESERVATION_SQL);
		deleteBookingStatement = conn.prepareStatement(DELETE_BOOKING_SQL);
		deleteUserStatement = conn.prepareStatement(DELETE_USER_SQL);
		
        checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);
		checkReservationRowStatement = conn.prepareStatement(CHECK_RESERVATION_ROW_NUMBER);

        /* add here more prepare statements for all the other queries you need */
        /* . . . . . . */
		userLoginStatement = conn.prepareStatement(LOGIN_SQL);
		
		createUserStatement = conn.prepareStatement(CREATE_USER_SQL);
		
		searchOneHopStatement = conn.prepareStatement(SEARCH_ONE_HOP_SQL);
		searchTwoHopStatement = conn.prepareStatement(SEARCH_TWO_HOP_SQL);
		
		checkBookingStatement = conn.prepareStatement(CHECK_BOOKING_SQL);
		newBookingStatement = conn.prepareStatement(NEW_BOOKING_SQL);
		updateBookingStatement = conn.prepareStatement(UPDATE_BOOKING_SQL);
		
		oneReservationStatement = conn.prepareStatement(ONE_RESERVATION_SQL);
		twoReservationStatement = conn.prepareStatement(TWO_RESERVATION_SQL);
		
		searchReservationStatement = conn.prepareStatement(SEARCH_RESERVATION_SQL);
		unpaidReservationStatement = conn.prepareStatement(SEARCH_UNPAID_SQL);
		
		searchFlightStatement = conn.prepareStatement(SEARCH_FLIGHT_SQL);
		
		updatePaidStatement = conn.prepareStatement(UPDATE_UNPAID_SQL);
		checkPriceStatement = conn.prepareStatement(FLIGHT_PRICE_SQL);
		updateBalanceStatement = conn.prepareStatement(UPDATE_BALANCE_SQL);
		checkBalanceStatement = conn.prepareStatement(SEARCH_BALANCE_SQL);
		unpaidReservationStatement2 = conn.prepareStatement(SEARCH_UNPAID_SQL_2);
		
		searchFirstFidStatement = conn.prepareStatement(FIRST_FID_SQL);
		searchSecondFidStatement = conn.prepareStatement(SECOND_FID_SQL);
		cancelReservationStatement = conn.prepareStatement(CANCEL_RESERVATION_SQL);
		itineraries = new ArrayList<ArrayList<Integer>>();
    }

    /**
     * Takes a user's username and password and attempts to log the user in.
     *
     * @param username
     * @param password
     *
     * @return If someone has already logged in, then return "User already logged in\n"
     * For all other errors, return "Login failed\n".
     *
     * Otherwise, return "Logged in as [username]\n".
     */
    public String transaction_login(String newUser, String password) {
		try {			
			if (username != null){
				return "User already logged in\n";
			}
			userLoginStatement.clearParameters();
			userLoginStatement.setString(1, newUser);
			userLoginStatement.setString(2, password);
			ResultSet loginResults = userLoginStatement.executeQuery();
			if (loginResults.next()){
				this.username = newUser;
				return "Logged in as " + newUser + "\n";
			}
			loginResults.close();
		} catch (SQLException e){
			return "Login failed\n";
		}
		return "Login failed\n";
    }

    /**
     * Implement the create user function.
     *
     * @param username new user's username. User names are unique the system.
     * @param password new user's password.
     * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
     *
     * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
     */
    public String transaction_createCustomer(String username, String password, int initAmount) {
		try {
			if (initAmount < 0) {
				return "Failed to create user\n";
			}
			createUserStatement.clearParameters();
			createUserStatement.setString(1, username);
			createUserStatement.setString(2, password);
			createUserStatement.setInt(3, initAmount);
			createUserStatement.executeUpdate();
			return "Created user " + username + "\n";
		} catch (SQLException e){
			return "Failed to create user\n";
		}
    }

    /**
     * Implement the search function.
     *
     * Searches for flights from the given origin city to the given destination
     * city, on the given day of the month. If {@code directFlight} is true, it only
     * searches for direct flights, otherwise is searches for direct flights
     * and flights with two "hops." Only searches for up to the number of
     * itineraries given by {@code numberOfItineraries}.
     *
     * The results are sorted based on total flight time.
     *
     * @param originCity
     * @param destinationCity
     * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
     * @param dayOfMonth
     * @param numberOfItineraries number of itineraries to return
     *
     * @return If no itineraries were found, return "No flights match your selection\n".
     * If an error occurs, then return "Failed to search\n".
     *
     * Otherwise, the sorted itineraries printed in the following format:
     *
     * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
     * [first flight in itinerary]\n
     * ...
     * [last flight in itinerary]\n
     *
     * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
     * in each search should always start from 0 and increase by 1.
     *
     * @see Flight#toString()
     */
    public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
        int numberOfItineraries) {
		
		try {
			
			if (directFlight){
				StringBuffer sb = new StringBuffer();
				itineraries.clear();
			
				//one hop
				searchOneHopStatement.clearParameters();
				searchOneHopStatement.setInt(1, numberOfItineraries);
				searchOneHopStatement.setString(2, originCity);
				searchOneHopStatement.setString(3, destinationCity);
				searchOneHopStatement.setInt(4, dayOfMonth);
				ResultSet oneHopResults = searchOneHopStatement.executeQuery();
				
				int row_id = 0;
				int flight_count = 0;
				while (oneHopResults.next() && flight_count < numberOfItineraries){
					row_id++;
					flight_count++;
					sb.append("Itinerary "+(row_id-1)+": 1 flight(s), "+ oneHopResults.getInt("total_time") +" minutes\n");				
					String s = "ID: " + oneHopResults.getInt("fid") + " Day: " + oneHopResults.getInt("day_of_month") +
						" Carrier: " + oneHopResults.getString("carrier_id") + " Number: " + oneHopResults.getString("flight_num") +
						" Origin: " + oneHopResults.getString("origin_city") + " Dest: " + oneHopResults.getString("dest_city") + 
						" Duration: " + oneHopResults.getInt("total_time") + " Capacity: " + oneHopResults.getInt("capacity") +
						" Price: " + oneHopResults.getInt("price") + "\n";
					sb.append(s);
					
					itineraries.add(new ArrayList<Integer>());
					int index = itineraries.size()-1;
					itineraries.get(index).add(oneHopResults.getInt("total_time"));
					itineraries.get(index).add(oneHopResults.getInt("day_of_month"));
					itineraries.get(index).add(oneHopResults.getInt("fid"));
					
				}
				//oneHopResults.close();
				
				//two hop
				//if (!directFlight && flight_count < numberOfItineraries){
				if (flight_count < numberOfItineraries){
					searchTwoHopStatement.clearParameters();
					searchTwoHopStatement.setInt(1, numberOfItineraries - flight_count);
					searchTwoHopStatement.setString(2, originCity);
					searchTwoHopStatement.setString(3, destinationCity);
					searchTwoHopStatement.setInt(4, dayOfMonth);
					ResultSet twoHopResults = searchTwoHopStatement.executeQuery();
					
					while(twoHopResults.next() && flight_count < numberOfItineraries){
						row_id++;
						flight_count++;
						
						sb.append("Itinerary "+(row_id-1)+": 2 flight(s), "+ twoHopResults.getInt("total_time") +" minutes\n");	
						String s1 = "ID: " + twoHopResults.getInt("fid1") + " Day: " + twoHopResults.getInt("day_of_month1") +
							" Carrier: " + twoHopResults.getString("carrier_id1") + " Number: " + twoHopResults.getString("flight_num1") +
							" Origin: " + twoHopResults.getString("origin_city1") + " Dest: " + twoHopResults.getString("dest_city1") + 
							" Duration: " + twoHopResults.getInt("actual_time1") + " Capacity: " + twoHopResults.getInt("capacity1") +
							" Price: " + twoHopResults.getInt("price1") + "\n";
						sb.append(s1);
						
						String s2 = "ID: " + twoHopResults.getInt("fid2") + " Day: " + twoHopResults.getInt("day_of_month2") +
							" Carrier: " + twoHopResults.getString("carrier_id2") + " Number: " + twoHopResults.getString("flight_num2") +
							" Origin: " + twoHopResults.getString("origin_city2") + " Dest: " + twoHopResults.getString("dest_city2") + 
							" Duration: " + twoHopResults.getInt("actual_time2") + " Capacity: " + twoHopResults.getInt("capacity2") +
							" Price: " + twoHopResults.getInt("price2") + "\n";
						sb.append(s2);
						
						itineraries.add(new ArrayList<Integer>());
						int index = itineraries.size()-1;
						itineraries.get(index).add(twoHopResults.getInt("total_time"));
						itineraries.get(index).add(twoHopResults.getInt("day_of_month1"));
						itineraries.get(index).add(twoHopResults.getInt("fid1"));
						itineraries.get(index).add(twoHopResults.getInt("fid2"));
						//itineraries.get(index).add(twoHopResults.getInt("day_of_month2"));
					}
					//twoHopResults.close();
				} 
				return sb.toString();
			}
			return null;
			/*
			else {
				StringBuffer sb = new StringBuffer();
				itineraries.clear();
			
				//one hop
				searchOneHopStatement.clearParameters();
				searchOneHopStatement.setInt(1, numberOfItineraries);
				searchOneHopStatement.setString(2, originCity);
				searchOneHopStatement.setString(3, destinationCity);
				searchOneHopStatement.setInt(4, dayOfMonth);
				ResultSet oneHopResults = searchOneHopStatement.executeQuery();
				
				int row_id = 0;
				int flight_count = 0;
				while (oneHopResults.next() && flight_count < numberOfItineraries){
					row_id++;
					flight_count++;

					
					itineraries.add(new ArrayList<Integer>());
					int index = itineraries.size()-1;
					itineraries.get(index).add(oneHopResults.getInt("total_time"));
					itineraries.get(index).add(oneHopResults.getInt("day_of_month"));
					itineraries.get(index).add(oneHopResults.getInt("fid"));
					
				}
				
				//two hop
				row_id = 0;
				flight_count = 0;
				searchTwoHopStatement.clearParameters();
				searchTwoHopStatement.setInt(1, numberOfItineraries);
				searchTwoHopStatement.setString(2, originCity);
				searchTwoHopStatement.setString(3, destinationCity);
				searchTwoHopStatement.setInt(4, dayOfMonth);
				ResultSet twoHopResults = searchTwoHopStatement.executeQuery();
				
				while(twoHopResults.next() && flight_count < numberOfItineraries){
					row_id++;
					flight_count++;

					itineraries.add(new ArrayList<Integer>());
					int index = itineraries.size()-1;
					itineraries.get(index).add(twoHopResults.getInt("total_time"));
					itineraries.get(index).add(twoHopResults.getInt("day_of_month1"));
					itineraries.get(index).add(twoHopResults.getInt("fid1"));
					itineraries.get(index).add(twoHopResults.getInt("fid2"));
				}
				
				//sort the itineraries based on total_time
				
				
				Collections.sort(itineraries, new Comparator<ArrayList<Integer>>() {
					@Override
					public int compare(ArrayList<Integer> lhs, ArrayList<Integer> rhs) {
						// -1 - less than, 1 - greater than, 0 - equal, all inversed for descending
						return lhs.get(0) > rhs.get(0) ? -1 : (lhs.get(0) < rhs.get(0)) ? 1 : 0;
					}
				});
				
				
				
				Comparator<ArrayList<Integer>> comp = (ArrayList<Integer> a, ArrayList<Integer> b) -> {
					return (b.get(0)).compareTo((a.get(0)));
				};

				Collections.sort(itineraries, comp);
				
				
				
				
				//iterate and append into string buffer
				for (int i = 0; i < itineraries.size(); i++){
					itinerary = itineraries.get(i);
					if (itinerary.size() == 3){
						// one hop flight
						int time = itinerary.get(0);
						int date = itinerary.get(1);
						int fid1 = itinerary.get(2);
						
						sb.append("Itinerary "+ (i+1) +": 1 flight(s), "+ time +"minutes\n");
						
						searchFlightStatement.clearParameters();
						searchFlightStatement.setInt(1, fid1);
						ResultSet f1 = searchFlightStatement.executeQuery();
						f1.next();
						String s1 = "ID: " + f1.getInt("fid") + " Day: " + f1.getInt("day_of_month") +
							" Carrier: " + f1.getString("carrier_id") + " Number: " + f1.getString("flight_num") +
							" Origin: " + f1.getString("origin_city") + " Dest: " + f1.getString("dest_city") + 
							" Duration: " + f1.getInt("actual_time") + " Capacity: " + f1.getInt("capacity") +
							" Price: " + f1.getInt("price") + "\n";
						sb.append(s1);
						f1.close();
						
					} else {
						// two hop flight
						int time = itinerary.get(0);
						int date = itinerary.get(1);
						int fid1 = itinerary.get(2);
						int fid2 = itinerary.get(3);
						
						sb.append("Itinerary "+ (i+1) +": 2 flight(s), "+ time +"minutes\n");
						
						searchFlightStatement.clearParameters();
						searchFlightStatement.setInt(1, fid1);
						ResultSet f1 = searchFlightStatement.executeQuery();
						f1.next();
						String s1 = "ID: " + f1.getInt("fid") + " Day: " + f1.getInt("day_of_month") +
							" Carrier: " + f1.getString("carrier_id") + " Number: " + f1.getString("flight_num") +
							" Origin: " + f1.getString("origin_city") + " Dest: " + f1.getString("dest_city") + 
							" Duration: " + f1.getInt("actual_time") + " Capacity: " + f1.getInt("capacity") +
							" Price: " + f1.getInt("price") + "\n";
						sb.append(s1);
						f1.close();

						searchFlightStatement.setInt(1, fid2);
						ResultSet f2 = searchFlightStatement.executeQuery();
						f2.next();
						
						String s2 = "ID: " + f2.getInt("fid") + " Day: " + f2.getInt("day_of_month") +
							" Carrier: " + f2.getString("carrier_id") + " Number: " + f2.getString("flight_num") +
							" Origin: " + f2.getString("origin_city") + " Dest: " + f2.getString("dest_city") + 
							" Duration: " + f2.getInt("actual_time") + " Capacity: " + f2.getInt("capacity") +
							" Price: " + f2.getInt("price") + "\n";
						sb.append(s2);
						f2.close();
						
					}
				}
			return sb.toString();
		*/
		} catch (SQLException e){
			e.printStackTrace();
			return "Failed to search\n";
		}
    }

    /**
     * Same as {@code transaction_search} except that it only performs single hop search and
     * do it in an unsafe manner.
     *
     * @param originCity
     * @param destinationCity
     * @param directFlight
     * @param dayOfMonth
     * @param numberOfItineraries
     *
     * @return The search results. Note that this implementation *does not conform* to the format required by
     * {@code transaction_search}.
     */
    private String transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight,
        int dayOfMonth, int numberOfItineraries) throws SQLException{
        StringBuffer sb = new StringBuffer();

        try {
            // one hop itineraries
            String unsafeSearchSQL =
                "SELECT TOP (" + numberOfItineraries + ") day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price " +
                "FROM Flights " +
                "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity + "\' AND day_of_month =  " + dayOfMonth + " " +
                "ORDER BY actual_time ASC";

            Statement searchStatement = conn.createStatement();
            ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);

            while (oneHopResults.next()) {
                int result_dayOfMonth = oneHopResults.getInt("day_of_month");
                String result_carrierId = oneHopResults.getString("carrier_id");
                String result_flightNum = oneHopResults.getString("flight_num");
                String result_originCity = oneHopResults.getString("origin_city");
                String result_destCity = oneHopResults.getString("dest_city");
                int result_time = oneHopResults.getInt("actual_time");
                int result_capacity = oneHopResults.getInt("capacity");
                int result_price = oneHopResults.getInt("price");

                sb.append("Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: " + result_flightNum + " Origin: " + result_originCity + " Destination: " + result_destCity + " Duration: " + result_time + " Capacity: " + result_capacity + " Price: " + result_price + "\n");
            }
            oneHopResults.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

    /**
     * Implements the book itinerary function.
     *
     * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
     *
     * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
     * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
     * If the user already has a reservation on the same day as the one that they are trying to book now, then return
     * "You cannot book two flights in the same day\n".
     * For all other errors, return "Booking failed\n".
     *
     * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
     * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
     * successful reservation is made by any user in the system.
     */
    public String transaction_book(int itineraryId) {
		try {
			if (username == null){
				return "Cannot book reservations, not logged in\n";
			} else if (itineraryId < 0 || itineraryId > itineraries.size()){
				return "No such itinerary " + itineraryId + "\n";
			}
			beginTransaction();
			
			ArrayList<Integer> itinerary = itineraries.get(itineraryId);

			int reservation_row_number = checkReservationRow();
			if (itinerary.size() == 3){
				int date = itinerary.get(1);
				int flight_id = itinerary.get(2);
				checkBookingStatement.clearParameters();
				checkBookingStatement.setInt(1, flight_id);
				ResultSet flightResults = checkBookingStatement.executeQuery();
				boolean booking = flightResults.next();
				int capacity = checkFlightCapacity(flight_id);
				if (capacity == 0){
					rollbackTransaction();
					return "Booking failed\n";
				}
				if (!booking) {
					newBookingStatement.clearParameters();
					newBookingStatement.setInt(1, flight_id);
					newBookingStatement.executeUpdate();
				} else if (capacity > flightResults.getInt("count")){
					updateBookingStatement.clearParameters();
					updateBookingStatement.setInt(1, flightResults.getInt("count")+1);
					updateBookingStatement.setInt(2, flight_id);
					updateBookingStatement.executeUpdate();
				} else {
					rollbackTransaction();
					return "Booking failed\n";
				}
				oneReservationStatement.setString(1, username);
				oneReservationStatement.setInt(2, reservation_row_number+1);
				oneReservationStatement.setInt(3, flight_id);
				oneReservationStatement.setInt(4, date);
				oneReservationStatement.setString(5, "false");
			
				try {
					oneReservationStatement.executeUpdate();
				} catch (SQLException e) {
					//e.printStackTrace();
					rollbackTransaction();
					return "You cannot book two flights in the same day\n";
				}
				commitTransaction();
			} else {
				// non direct flights
				
				int date = itinerary.get(1);
				int flight_id_1 = itinerary.get(2);
				int flight_id_2 = itinerary.get(3);
		
				//check flight 1 capacity
				checkBookingStatement.clearParameters();
				checkBookingStatement.setInt(1, flight_id_1);
				ResultSet flightResults1 = checkBookingStatement.executeQuery();
				boolean booking1 = flightResults1.next();
				int capacity1 = checkFlightCapacity(flight_id_1);
				
				if (capacity1 == 0){
					rollbackTransaction();
					return "Booking failed\n";
				}
				
				if (!booking1) {
					newBookingStatement.clearParameters();
					newBookingStatement.setInt(1, flight_id_1);
					newBookingStatement.executeUpdate();
				} else if (capacity1 > flightResults1.getInt("count")){
					updateBookingStatement.clearParameters();
					updateBookingStatement.setInt(1, flightResults1.getInt("count")+1);
					updateBookingStatement.setInt(2, flight_id_1);
					updateBookingStatement.executeUpdate();
				} else {
					rollbackTransaction();
					return "Booking failed\n";
				}
				
				//check flight 2 capacity
				checkBookingStatement.clearParameters();
				checkBookingStatement.setInt(1, flight_id_2);
				ResultSet flightResults2 = checkBookingStatement.executeQuery();
				boolean booking2 = flightResults2.next();
				int capacity2 = checkFlightCapacity(flight_id_2);
				
				if (capacity2 == 0){
					rollbackTransaction();
					return "Booking failed\n";
				}
				
				if (!booking2) {
					newBookingStatement.clearParameters();
					newBookingStatement.setInt(1, flight_id_2);
					newBookingStatement.executeUpdate();
				} else if (capacity2 > flightResults2.getInt("count")){
					updateBookingStatement.clearParameters();
					updateBookingStatement.setInt(1, flightResults2.getInt("count")+1);
					updateBookingStatement.setInt(2, flight_id_2);
					updateBookingStatement.executeUpdate();
				} else {
					rollbackTransaction();
					return "Booking failed\n";
				}
				//flightResults1.close();
				
				
				twoReservationStatement.setString(1, username);
				twoReservationStatement.setInt(2, reservation_row_number+1);
				twoReservationStatement.setInt(3, flight_id_1);
				twoReservationStatement.setInt(4, flight_id_2);
				twoReservationStatement.setInt(5, date);
				twoReservationStatement.setString(6, "false");
				
				try {
					twoReservationStatement.executeUpdate();
				} catch (SQLException e) {
					e.printStackTrace();
					rollbackTransaction();
					return "You cannot book two flights in the same day\n";
				}
				commitTransaction();
			}
			return "Booked flight(s), reservation ID: " + (reservation_row_number+1) + "\n";
		} catch (SQLException e){
			e.printStackTrace();
			//rollbackTransaction();
			return "Booking failed\n";
		}
    }

	/*
	private static boolean isConstraintViolation(SQLException e) throws SQLException{
		return e.getSQLState().startsWith("23");
	}
	*/
	
    /**
     * Implements the reservations function.
     *
     * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
     * If the user has no reservations, then return "No reservations found\n"
     * For all other errors, return "Failed to retrieve reservations\n"
     *
     * Otherwise return the reservations in the following format:
     *
     * Reservation [reservation ID] paid: [true or false]:\n"
     * [flight 1 under the reservation]
     * [flight 2 under the reservation]
     * Reservation [reservation ID] paid: [true or false]:\n"
     * [flight 1 under the reservation]
     * [flight 2 under the reservation]
     * ...
     *
     * Each flight should be printed using the same format as in the {@code Flight} class.
     *
     * @see Flight#toString()
     */
    public String transaction_reservations() {
		try {
			StringBuffer sb = new StringBuffer();
			if (username == null){
				return "Cannot view reservations, not logged in\n";
			} 
			searchReservationStatement.clearParameters();
			searchReservationStatement.setString(1, username);
			ResultSet reservation_list = searchReservationStatement.executeQuery();
				
			if (reservation_list.isBeforeFirst()){
				while (reservation_list.next()){
					int rid = reservation_list.getInt("rid");
					int fid1 = reservation_list.getInt("fid1");
					int fid2 = -1; //place holder
					searchSecondFidStatement.clearParameters();
					searchSecondFidStatement.setString(1, username);
					searchSecondFidStatement.setInt(2, rid);
					ResultSet secondID = searchSecondFidStatement.executeQuery();
					//boolean hasSecondFlight = false;
					if (secondID.next()){
						//hasSecondFlight = true;
						
						fid2 = secondID.getInt("fid2");
						
					}
					
					String paid = reservation_list.getString("paid");
					sb.append("Reservation " + rid + " paid: " + paid + ":\n");
					
					searchFlightStatement.clearParameters();
					searchFlightStatement.setInt(1, fid1);
					ResultSet f1 = searchFlightStatement.executeQuery();
					f1.next();
					String s1 = "ID: " + f1.getInt("fid") + " Day: " + f1.getInt("day_of_month") +
						" Carrier: " + f1.getString("carrier_id") + " Number: " + f1.getString("flight_num") +
						" Origin: " + f1.getString("origin_city") + " Dest: " + f1.getString("dest_city") + 
						" Duration: " + f1.getInt("actual_time") + " Capacity: " + f1.getInt("capacity") +
						" Price: " + f1.getInt("price") + "\n";
					sb.append(s1);
					f1.close();
					if (fid2 != 0){
						searchFlightStatement.setInt(1, fid2);
						ResultSet f2 = searchFlightStatement.executeQuery();
						f2.next();
						
						String s2 = "ID: " + f2.getInt("fid") + " Day: " + f2.getInt("day_of_month") +
							" Carrier: " + f2.getString("carrier_id") + " Number: " + f2.getString("flight_num") +
							" Origin: " + f2.getString("origin_city") + " Dest: " + f2.getString("dest_city") + 
							" Duration: " + f2.getInt("actual_time") + " Capacity: " + f2.getInt("capacity") +
							" Price: " + f2.getInt("price") + "\n";
						sb.append(s2);
						f2.close();
					}
				}
			} else {
				return "No reservations found\n";
			}

			return sb.toString();
		} catch (SQLException e) {
			e.printStackTrace();
			return "Failed to retrieve reservations\n";
		}
    }

    /**
     * Implements the cancel operation.
     *
     * @param reservationId the reservation ID to cancel
     *
     * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
     * For all other errors, return "Failed to cancel reservation [reservationId]"
     *
     * If successful, return "Canceled reservation [reservationId]"
     *
     * Even though a reservation has been canceled, its ID should not be reused by the system.
     */
	 
	
    public String transaction_cancel(int reservationId) {
        // only implement this if you are interested in earning extra credit for the HW!
		try {
			if (username == null){
				return "Cannot pay, not logged in\n";
			}
			beginTransaction();

			searchFirstFidStatement.clearParameters();
			searchFirstFidStatement.setString(1, username);
			searchFirstFidStatement.setInt(2, reservationId);
			ResultSet cancel = searchFirstFidStatement.executeQuery();
			
			searchSecondFidStatement.clearParameters();
			searchSecondFidStatement.setString(1, username);
			searchSecondFidStatement.setInt(2, reservationId);
			ResultSet cancel2 = searchSecondFidStatement.executeQuery();
			
			if (cancel.isBeforeFirst()){
				cancel.next();
				cancelReservationStatement.setString(1, username);
				cancelReservationStatement.setInt(2, reservationId);
				cancelReservationStatement.executeUpdate();
				
				int firstFID = cancel.getInt("fid1");
				checkBookingStatement.clearParameters();
				checkBookingStatement.setInt(1, firstFID);
				ResultSet f1 = checkBookingStatement.executeQuery();
				int current1 = f1.getInt("count");
				updateBookingStatement.clearParameters();
				updateBookingStatement.setInt(1, current1-1);
				updateBookingStatement.setInt(2, firstFID);
				updateBookingStatement.executeQuery();
			} else {
				rollbackTransaction();
			}
			if (cancel2.next()){
				int secondFID = cancel2.getInt("fid2");
				checkBookingStatement.clearParameters();
				checkBookingStatement.setInt(1, secondFID);
				ResultSet f2 = checkBookingStatement.executeQuery();
				int current2 = f2.getInt("count");
				updateBookingStatement.clearParameters();
				updateBookingStatement.setInt(1, current2-1);
				updateBookingStatement.setInt(2, secondFID);
				updateBookingStatement.executeQuery();
			}
			commitTransaction();
			return "Canceled reservation " + reservationId + "\n";

		} catch (SQLException e){
			//rollbackTransaction();
			return "Failed to cancel reservation " + reservationId;
		}
    }
	

    /**
     * Implements the pay function.
     *
     * @param reservationId the reservation to pay for.
     *
     * @return If no user has logged in, then return "Cannot pay, not logged in\n"
     * If the reservation is not found / not under the logged in user's name, then return
     * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
     * If the user does not have enough money in their account, then return
     * "User has only [balance] in account but itinerary costs [cost]\n"
     * For all other errors, return "Failed to pay for reservation [reservationId]\n"
     *
     * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
     * where [balance] is the remaining balance in the user's account.
     */
    public String transaction_pay(int reservationId) {
		try {
			if (username == null){
				return "Cannot pay, not logged in\n";
			}
			beginTransaction();
			
			//query for the user balance 
			checkBalanceStatement.clearParameters();
			checkBalanceStatement.setString(1, username);
			ResultSet b = checkBalanceStatement.executeQuery();
			b.next();
			int user_balance = b.getInt("balance");
			
			//get the price of the first flight
			unpaidReservationStatement.clearParameters();
			unpaidReservationStatement.setString(1, username);
			unpaidReservationStatement.setInt(2, reservationId);
			ResultSet unpaid = unpaidReservationStatement.executeQuery();
			
			int total_price = 0;
			if (!unpaid.next()){
				rollbackTransaction();
				return "Cannot find unpaid reservation "+reservationId+" under user: "+username+"\n";
			} else {
				//unpaid.next() ?? maybe not sure

				int fid1 = unpaid.getInt("fid1");
				int fid2 = unpaid.getInt("fid2");
				checkPriceStatement.clearParameters();
				checkPriceStatement.setInt(1, fid1);
				ResultSet p1 = checkPriceStatement.executeQuery();
				p1.next();
				total_price += p1.getInt("price");
				p1.close();
				
				//unpaidReservationStatement2.clearParameters();
				//unpaidReservationStatement2.setString(1, username);
				//unpaidReservationStatement2.setInt(2, reservationId);
				//ResultSet second = unpaidReservationStatement2.executeQuery();
				if (fid2 != 0){
					//second fid is not null
					checkPriceStatement.clearParameters();
					checkPriceStatement.setInt(1, fid2);
					ResultSet p2 = checkPriceStatement.executeQuery();
					p2.next();
					total_price += p2.getInt("price");
					p2.close();
				}
				
				if (total_price > user_balance){
					rollbackTransaction();
					return "User has only "+user_balance+" in account but itinerary costs "+total_price+"\n";
				} else {					
					
					updatePaidStatement.clearParameters();
					updatePaidStatement.setString(1, "true");
					updatePaidStatement.setInt(2, reservationId);
					updatePaidStatement.executeUpdate();
					
					updateBalanceStatement.clearParameters();
					updateBalanceStatement.setInt(1, (user_balance-total_price));
					updateBalanceStatement.setString(2, username);
					updateBalanceStatement.executeUpdate();
					
					commitTransaction();
					return "Paid reservation: "+reservationId+" remaining balance: "+(user_balance-total_price)+"\n";
				}
			}
		} catch (SQLException e){
			e.printStackTrace();
			//rollbackTransaction();
			return "Failed to pay for reservation " + reservationId + "\n";
		}
    }


    /* some utility functions below */

    public void beginTransaction() throws SQLException {
        conn.setAutoCommit(false);
        beginTransactionStatement.executeUpdate();
    }

    public void commitTransaction() throws SQLException {
        commitTransactionStatement.executeUpdate();
        conn.setAutoCommit(true);
    }

    public void rollbackTransaction() throws SQLException {
        rollbackTransactionStatement.executeUpdate();
        conn.setAutoCommit(true);
    }

    /**
     * Shows an example of using PreparedStatements after setting arguments. You don't need to
     * use this method if you don't want to.
     */
    private int checkFlightCapacity(int fid) throws SQLException {
        checkFlightCapacityStatement.clearParameters();
        checkFlightCapacityStatement.setInt(1, fid);
        ResultSet results = checkFlightCapacityStatement.executeQuery();
        results.next();
        int capacity = results.getInt("capacity");
        results.close();

        return capacity;
    }
	
	private int checkReservationRow() throws SQLException {
		checkReservationRowStatement.clearParameters();
		ResultSet results = checkReservationRowStatement.executeQuery();
		results.next();
		int row_number = results.getInt("row_number");
		results.close();
		return row_number;
	}
}



