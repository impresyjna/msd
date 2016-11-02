package main.java.main;

import models.Queries;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Impresyjna on 27.10.2016.
 */
public class Main {
    public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException {
        Queries queriesString = new Queries();
        // load the sqlite-JDBC driver using the current class loader
        Class.forName("org.sqlite.JDBC");
        Connection connection = null;
        BufferedReader uniqueTracks = new BufferedReader(new InputStreamReader(new FileInputStream("unique_tracks.txt"), "ISO-8859-1"));
// create a database connection
        connection = DriverManager.getConnection("jdbc:sqlite:msd.db");
        connection.setAutoCommit(false);

        Statement statement = connection.createStatement();

        statement.executeUpdate(queriesString.dropTrackBeforeTable);
        statement.executeUpdate(queriesString.createTrackBeforeTable);
        statement.executeUpdate(queriesString.dropTrackTable);
        statement.executeUpdate(queriesString.createTrackeTable);
        statement.executeUpdate(queriesString.dropArtistTable);
        statement.executeUpdate(queriesString.createArtistTable);
        statement.executeUpdate(queriesString.dropUserTable);
        statement.executeUpdate(queriesString.createUserTable);
        statement.executeUpdate(queriesString.dropTripletTable);
        statement.executeUpdate(queriesString.createTripletTable);
        statement.executeUpdate(queriesString.dropTripletBeforeTable);
        statement.executeUpdate(queriesString.createTripletBeforeTable);
        statement.executeUpdate(queriesString.dropDateTable);
        statement.executeUpdate(queriesString.createDateTable);

        //Counters
        int tracksCounter = 0;
        int artistsCounter = 0;
        int usersCounter = 0;
        int tripletsCounter = 0;
        int tripletsBeforeCounter = 0;
        int datesCounter = 0;

        //Maps
        Map<String, Integer> trackIdMap = new HashMap<String, Integer>();
        Map<String, Integer> artistIdMap = new HashMap<String, Integer>();
        Map<String, Integer> userIdMap = new HashMap<String, Integer>();
        Map<String, Integer> dateIdMap = new HashMap<String, Integer>();
        Map<String, Integer> tripletIdMap = new HashMap<String, Integer>();
        Map<String, String> artistTrackMap = new HashMap<String, String>();

        //Strings
        final String insertIntoTracksBefore = "insert into tracks_before (track_id, song_id, artist, title) values (?,?,?,?)";
        final String insertIntoTracks = "insert into tracks (track_id, song_id, title) values (?,?,?)";
        final String insertIntoArtists = "insert into artists (artist) values (?)";
        final String insertIntoTripletsBefore = "insert into triplets_before (user_id, track_id, timestamp) values (?,?,?)";
        final String insertIntoTriplets = "insert into triplets (user_id, track_id, artist_id, datum_id) values (?,?,?,?)";
        final String insertIntoUsers = "insert into users (old_id) values (?)";
        final String insertIntoDatums = "insert into datums (year, month, day) values (?, ?, ?)";

        //PreparedStatements
        PreparedStatement trackBeforeStatement = connection.prepareStatement(insertIntoTracksBefore);
        PreparedStatement trackStatement = connection.prepareStatement(insertIntoTracks);
        PreparedStatement artistStatement = connection.prepareStatement(insertIntoArtists);
        PreparedStatement tripletBeforeStatement = connection.prepareStatement(insertIntoTripletsBefore);
        PreparedStatement tripletStatement = connection.prepareStatement(insertIntoTriplets);
        PreparedStatement userStatement = connection.prepareStatement(insertIntoUsers);
        PreparedStatement datumStatement = connection.prepareStatement(insertIntoDatums);

        long startTime = System.nanoTime();
        long elapsedTime;

        String line;
        while ((line = uniqueTracks.readLine()) != null) {
            String[] parts = line.split("<SEP>");
            if (parts.length == 4) {
                //Add to TracksBefore
                trackBeforeStatement.setString(1, parts[0]);
                trackBeforeStatement.setString(2, parts[1]);
                trackBeforeStatement.setString(3, parts[2]);
                trackBeforeStatement.setString(4, parts[3]);
                trackBeforeStatement.addBatch();
                //Add to Tracks
                trackStatement.setString(1, parts[0]);
                trackStatement.setString(2, parts[1]);
                trackStatement.setString(3, parts[3]);
                trackStatement.addBatch();
                tracksCounter += 1;

                if (trackIdMap.get(parts[1]) == null) {
                    trackIdMap.put(parts[1], tracksCounter);
                }
                if (artistIdMap.get(parts[2]) == null) {
                    artistStatement.setString(1, parts[2]);
                    artistStatement.addBatch();
                    artistsCounter += 1;
                    artistIdMap.put(parts[2], artistsCounter);
                }
                artistTrackMap.put(parts[1], parts[2]);
            }
        }
        uniqueTracks.close();
        trackBeforeStatement.executeBatch();
        trackStatement.executeBatch();
        artistStatement.executeBatch();
        connection.commit();
        trackBeforeStatement.clearBatch();
        trackBeforeStatement.close();
        trackStatement.clearBatch();
        trackStatement.close();
        artistStatement.clearBatch();
        artistStatement.close();

        elapsedTime = System.nanoTime() - startTime;
        System.out.println("Finished unique_tracks in time " + elapsedTime / 1000000000 + " s");
        startTime = System.nanoTime();

        BufferedReader triplets = new BufferedReader(new InputStreamReader(new FileInputStream("triplets_sample_20p.txt")));
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        while ((line = triplets.readLine()) != null) {
            String[] parts = line.split("<SEP>");
            if (parts.length == 3) {
                //Add to TripletsBefore
                tripletBeforeStatement.setString(1, parts[0]);
                tripletBeforeStatement.setString(2, parts[1]);
                tripletBeforeStatement.setString(3, parts[2]);
                tripletBeforeStatement.addBatch();
                tripletsBeforeCounter++;
                cal.setTimeInMillis(Long.parseLong(parts[2]) * 1000);

                if (userIdMap.get(parts[0]) == null) {
                    userStatement.setString(1, parts[0]);
                    userStatement.addBatch();
                    usersCounter += 1;
                    userIdMap.put(parts[0], usersCounter);
                }
                if (!dateIdMap.containsKey(dateFormat.format(cal.getTime()))) {
                    datesCounter++;
                    datumStatement.setInt(1, cal.get(Calendar.YEAR));
                    datumStatement.setInt(2, cal.get(Calendar.MONTH));
                    datumStatement.setInt(3, cal.get(Calendar.DAY_OF_MONTH));
                    datumStatement.addBatch();
                    dateIdMap.put(dateFormat.format(cal.getTime()), datesCounter);
                }
                try {
                    tripletStatement.setInt(1, userIdMap.get(parts[0]));
                    tripletStatement.setInt(2, trackIdMap.get(parts[1]));
                    tripletStatement.setInt(3, artistIdMap.get(artistTrackMap.get(parts[1])));
                    tripletStatement.setInt(4, dateIdMap.get(dateFormat.format(cal.getTime())));
                    tripletStatement.addBatch();
                    tripletsCounter += 1;
                } catch (Exception e) {
                }
            }
            if (tripletsBeforeCounter >= 1000000) {
                tripletBeforeStatement.executeBatch();
                tripletStatement.executeBatch();
                userStatement.executeBatch();
                datumStatement.executeBatch();
                connection.commit();
                tripletBeforeStatement.clearBatch();
                tripletStatement.clearBatch();
                userStatement.clearBatch();
                datumStatement.clearBatch();
                tripletsBeforeCounter = 0;
            }
        }
        triplets.close();
        tripletBeforeStatement.executeBatch();
        tripletStatement.executeBatch();
        userStatement.executeBatch();
        datumStatement.executeBatch();
        connection.commit();
        tripletBeforeStatement.clearBatch();
        tripletStatement.clearBatch();
        userStatement.clearBatch();
        datumStatement.clearBatch();
        tripletBeforeStatement.close();
        tripletStatement.close();
        userStatement.close();
        datumStatement.close();
        statement.execute(queriesString.indexOnArtistIdFkCreateQuery);
        statement.execute(queriesString.indexOnDatumIdFkCreateQuery);
        statement.execute(queriesString.indexOnTripletIdFkCreateQuery);
        statement.execute(queriesString.indexOnUserIdFkCreateQuery);
        connection.commit();
        elapsedTime = System.nanoTime() - startTime;


        System.out.println("Finished triplets_sample in time " + elapsedTime / 1000000000 + " s");

        try {
            if (connection != null)
                connection.close();
        } catch (SQLException e) {
            // connection close failed.
            System.err.println(e);
        }
    }
}
