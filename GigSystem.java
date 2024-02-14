import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Savepoint;

import java.io.IOException;
import java.util.Properties;

import java.time.LocalDateTime;
import java.sql.Timestamp;
import java.util.Vector;

public class GigSystem {

    public static void main(String[] args) {

        // You should only need to fetch the connection details once
        // You might need to change this to either getSocketConnection() or getPortConnection() - see below
        Connection conn = getSocketConnection();

        boolean repeatMenu = true;
        
        while(repeatMenu){
            System.out.println("_________________________");
            System.out.println("________GigSystem________");
            System.out.println("_________________________");

            
            System.out.println("q: Quit");
            System.out.println("2: Option 2");
            System.out.println("3: Option 3");
            System.out.println("4: Option 4");

            String menuChoice = readEntry("Please choose an option: ");

            if(menuChoice.length() == 0){
                //Nothing was typed (user just pressed enter) so start the loop again
                continue;
            }
            char option = menuChoice.charAt(0);

            /**
             * If you are going to implement a menu, you must read input before you call the actual methods
             * Do not read input from any of the actual option methods
             */
            switch(option){
                case '1':
                    break;
                case '2': 
                        int[] actIDs = {1,2,3,4};
                        int[] fees = {1,2,3,4};
                        LocalDateTime[] onTimes = {LocalDateTime.of(2011,9,7,20,00),LocalDateTime.of(2011,9,7,20,20),LocalDateTime.of(2011,9,7,20,35),LocalDateTime.of(2011,9,7,20,45)};
                        int[] duration = {1,1,1,1};
                        option2(conn, "Big Hall", "Ambar's", actIDs, fees, onTimes, duration, 2);
                    break;
                case '3': 
                        option3(conn, 1, "xd", "xd@email.com","A");
                        option3(conn, 1, "xd", "xd@email.com","A");
                        option3(conn, 1, "xd", "xd@email.com","A");
                        option3(conn, 2, "ad", "xd@email.com","A");
                        option3(conn, 3, "xd", "xd@email.com","A");
                        option3(conn, 4, "xd", "xd@email.com","A");
                    break;
                case '4':
                        option4(conn, 1, "Scalar Swift");
                    break;
                case '5':
                    break;
                case '6': 
                    break;
                case '7':
                    break;
                case '8':
                    break;
                case 'q':
                    repeatMenu = false;
                    break;
                default: 
                    System.out.println("Invalid option");
            }
        }
    }


    public static String[][] option1(Connection conn, int gigID){
        String[][] result = null;
        String selectQuery = "SELECT actname, ontime::time, offtime(ontime, duration)::TIME FROM act_gig JOIN act USING(actID) WHERE gigID=? ORDER BY ontime";
        try{
            PreparedStatement preparedStatement = conn.prepareStatement(selectQuery);
            preparedStatement.setInt(1, gigID);
            ResultSet gigs = preparedStatement.executeQuery();
            result = convertResultToStrings(gigs);
            preparedStatement.close();
            gigs.close();
            return result;
        } catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public static void option2(Connection conn, String venue, String gigTitle, int[] actIDs, int[] fees, LocalDateTime[] onTimes, int[] durations, int adultTicketPrice){
        try{
            conn.setAutoCommit(false);
            //Savepoint set to rollback to in case of any invalid conditions
            Savepoint s = conn.setSavepoint();
            //Procedure OptionTWO(venueid, gigTitle, gigdate, cost) 
            //Adds (DEFAULT, venueID, gigtitle, gigdate, "GoingAhead") to gig
            //Adds (gigID, "A", adultTicketPrice) to gig_ticket
            PreparedStatement insertStatement = conn.prepareStatement("Call OptionTWO(retrieveVID(?), ?,  ?, ?)");            
            insertStatement.setString(1, venue);
            insertStatement.setString(2, gigTitle);
            Timestamp timestamp1 = Timestamp.valueOf(onTimes[0]);
            insertStatement.setTimestamp(3, timestamp1);
            insertStatement.setInt(4, adultTicketPrice);
            insertStatement.executeUpdate();
            insertStatement.close();
            //Loops through the actID and add it to act_gig
            PreparedStatement insertStatement2 = conn.prepareStatement("INSERT INTO act_gig VALUES (?, retrieveGID(?), ?, ?, ?) ON CONFLICT DO NOTHING");
            Timestamp timestamp2 = Timestamp.valueOf(onTimes[0]);
            for(int i = 0; i < actIDs.length; i++) {
                insertStatement2.setInt(1, actIDs[i]);
                insertStatement2.setTimestamp(2, timestamp1);
                insertStatement2.setInt(3, fees[i]);
                timestamp2 = Timestamp.valueOf(onTimes[i]);
                insertStatement2.setTimestamp(4, timestamp2);
                insertStatement2.setInt(5, durations[i]);
                insertStatement2.addBatch();
            }
            insertStatement2.executeBatch();
            insertStatement2.close();
            //Function available(gigdate, venuename)
            //Checks if there has been any overlaps in the case of booking such venue
            PreparedStatement updateStatement1 = conn.prepareStatement("SELECT available(?, ?)");
            updateStatement1.setTimestamp(1, timestamp1);
            updateStatement1.setString(2, venue);
            ResultSet truth = updateStatement1.executeQuery();
            boolean result = false;
            //Retrive the result if true then no overlaps has occured
            while(truth.next()) {
                result = truth.getBoolean(1);
            }
            if(result == false) {
                conn.rollback(s);
            }
            conn.commit();
            conn.setAutoCommit(true);
            updateStatement1.close();
        } catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }

    }

    public static void option3(Connection conn, int gigid, String name, String email, String ticketType){ 
        try{
            PreparedStatement insertStatement = conn.prepareStatement("CALL optionTHREE(?, ?, ?, ?)"); 
            insertStatement.setInt(1, gigid);    
            insertStatement.setString(2, ticketType);    
            insertStatement.setString(3, name);
            insertStatement.setString(4, email);
            insertStatement.executeUpdate();
            insertStatement.close();
        } catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }
    }

    public static String[] option4(Connection conn, int gigID, String actName){
        String[] emailsArray = null;
        try{
            PreparedStatement updateStatement = conn.prepareStatement("CALL optionFOUR(?, ?)"); 
            PreparedStatement queryStatement = conn.prepareStatement("SELECT CustomerEmail FROM ticket JOIN gig USING(gigID) WHERE gigstatus = 'Cancelled' AND gig.gigID = ?)"); 
            PreparedStatement queryStatement2 = conn.prepareStatement("SELECT count(*) FROM ticket JOIN gig USING(gigID) WHERE gigstatus = 'Cancelled' AND gig.gigID = ?)"); 
            queryStatement.setInt(1, gigID);    
            queryStatement2.setInt(1, gigID);    
            updateStatement.setInt(1, gigID);    
            updateStatement.setString(2, actName);    
            updateStatement.executeUpdate();
            ResultSet emails = queryStatement.executeQuery();
            ResultSet size = queryStatement2.executeQuery();
            int count = 0;
            while(size.next()) {
                count = size.getInt(1);
            }
            queryStatement.close();    
            queryStatement2.close();    
            updateStatement.close();
            int j = 1;
            if(count > 0) {
                emailsArray = new String[count];
                while(emails.next()) {
                    emailsArray[j] = emails.getString(j);
                    j++;
                }
            }
            
        } catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }
        return emailsArray;
    }

    public static String[][] option5(Connection conn){
        String[][] result = null;
        String selectQuery = "SELECT gigID, ticketneeded(gigID) FROM gig";
        try{
            PreparedStatement preparedStatement = conn.prepareStatement(selectQuery);
            ResultSet tickets = preparedStatement.executeQuery();
            result = convertResultToStrings(tickets);
            preparedStatement.close();
            tickets.close();
            printTable(result);
        } catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }
        return result;
    }

    public static String[][] option6(Connection conn){
        String[][] result = null;
        String selectQuery = "SELECT * FROM optionSIXVIEW WHERE ActName != 'Total'";
        try{
            PreparedStatement preparedStatement = conn.prepareStatement(selectQuery);
            ResultSet tickets = preparedStatement.executeQuery();
            result = convertResultToStrings(tickets);
            preparedStatement.close();
            tickets.close();
            printTable(result);
        } catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }
        return result;
    }

    public static String[][] option7(Connection conn){
        String[][] result = null;
        String selectQuery = "SELECT * FROM optionSEVENVIEW";
        try{
            PreparedStatement preparedStatement = conn.prepareStatement(selectQuery);
            ResultSet regularCust = preparedStatement.executeQuery();
            result = convertResultToStrings(regularCust);
            preparedStatement.close();
            regularCust.close();
        } catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }
        return result;
    }

    public static String[][] option8(Connection conn){
        String[][] result = null;
        String selectQuery = "SELECT * FROM optionEIGHTVIEW";
        try{
            PreparedStatement preparedStatement = conn.prepareStatement(selectQuery);
            ResultSet gigs = preparedStatement.executeQuery();
            result = convertResultToStrings(gigs);
            preparedStatement.close();
            gigs.close();
        } catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Prompts the user for input
     * @param prompt Prompt for user input
     * @return the text the user typed
     */

    private static String readEntry(String prompt) {
        
        try {
            StringBuffer buffer = new StringBuffer();
            System.out.print(prompt);
            System.out.flush();
            int c = System.in.read();
            while(c != '\n' && c != -1) {
                buffer.append((char)c);
                c = System.in.read();
            }
            return buffer.toString().trim();
        } catch (IOException e) {
            return "";
        }

    }
     
    /**
    * Gets the connection to the database using the Postgres driver, connecting via unix sockets
    * @return A JDBC Connection object
    */
    public static Connection getSocketConnection(){
        Properties props = new Properties();
        props.setProperty("socketFactory", "org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg");
        props.setProperty("socketFactoryArg",System.getenv("HOME") + "/cs258-postgres/postgres/tmp/.s.PGSQL.5432");
        Connection conn;
        try{
          conn = DriverManager.getConnection("jdbc:postgresql://localhost/cwk", props);
          return conn;
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Gets the connection to the database using the Postgres driver, connecting via TCP/IP port
     * @return A JDBC Connection object
     */
    public static Connection getPortConnection() {
        
        String user = "postgres";
        String passwrd = "password";
        Connection conn;

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException x) {
            System.out.println("Driver could not be loaded");
        }

        try {
            conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/cwk?user="+ user +"&password=" + passwrd);
            return conn;
        } catch(SQLException e) {
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
            System.out.println("Error retrieving connection");
            return null;
        }
    }

    public static String[][] convertResultToStrings(ResultSet rs){
        Vector<String[]> output = null;
        String[][] out = null;
        try {
            int columns = rs.getMetaData().getColumnCount();
            output = new Vector<String[]>();
            int rows = 0;
            while(rs.next()){
                String[] thisRow = new String[columns];
                for(int i = 0; i < columns; i++){
                    thisRow[i] = rs.getString(i+1);
                }
                output.add(thisRow);
                rows++;
            }
            // System.out.println(rows + " rows and " + columns + " columns");
            out = new String[rows][columns];
            for(int i = 0; i < rows; i++){
                out[i] = output.get(i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return out;
    }

    public static void printTable(String[][] out){
        int numCols = out[0].length;
        int w = 20;
        int widths[] = new int[numCols];
        for(int i = 0; i < numCols; i++){
            widths[i] = w;
        }
        printTable(out,widths);
    }

    public static void printTable(String[][] out, int[] widths){
        for(int i = 0; i < out.length; i++){
            for(int j = 0; j < out[i].length; j++){
                System.out.format("%"+widths[j]+"s",out[i][j]);
                if(j < out[i].length - 1){
                    System.out.print(",");
                }
            }
            System.out.println();
        }
    }

}