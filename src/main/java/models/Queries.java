package models;

/**
 * Created by Impresyjna on 27.10.2016.
 */
public class Queries {
    static public String dropTrackBeforeTable = "drop table if exists tracks_before";
    static public String createTrackBeforeTable = "create table tracks_before (track_id string NOT NULL PRIMARY KEY, song_id string, artist string, title string)";

    static public String dropTrackTable = "drop table if exists tracks";
    static public String createTrackeTable = "create table tracks (ID INTEGER PRIMARY KEY AUTOINCREMENT, track_id string, song_id string, title string)";

    static public String dropArtistTable = "drop table if exists artists";
    static public String createArtistTable = "create table artists (ID INTEGER PRIMARY KEY AUTOINCREMENT,  artist string)";

    static public String dropUserTable = "drop table if exists users";
    static public String createUserTable = "create table users (ID INTEGER PRIMARY KEY AUTOINCREMENT, old_id string)";

    static public String dropDateTable = "drop table if exists datums";
    static public String createDateTable = "create table datums (ID INTEGER PRIMARY KEY AUTOINCREMENT, year INTEGER, month INTEGER, day INTEGER)";

    static public String dropTripletTable = "drop table if exists triplets";
    static public String createTripletTable = "create table triplets (ID INTEGER PRIMARY KEY AUTOINCREMENT,"+
            "user_id INTEGER NOT NULL," +
            "track_id INTEGER NOT NULL," +
            "artist_id INTEGER NOT NULL," +
            "datum_id INTEGER NOT NULL," +
            "FOREIGN KEY (user_id) REFERENCES users(ID)," +
            "FOREIGN KEY (track_id) REFERENCES tracks(ID)," +
            "FOREIGN KEY (artist_id) REFERENCES artists(ID)," +
            "FOREIGN KEY (datum_id) REFERENCES datums(ID))";

    static public String dropTripletBeforeTable = "drop table if exists triplets_before";
    static public String createTripletBeforeTable = "create table triplets_before (ID INTEGER PRIMARY KEY AUTOINCREMENT, user_id string, track_id string, timestamp BIGINT)";

    //New schema indexes queries
    static public String indexOnTripletIdFkCreateQuery = "CREATE INDEX TRIPLET_ID_FK ON TRIPLETS (TRACK_ID)";
    static public String indexOnArtistIdFkCreateQuery = "CREATE INDEX ARTIST_ID_FK ON TRIPLETS (ARTIST_ID)";
    static public String indexOnDatumIdFkCreateQuery = "CREATE INDEX DATUM_ID_FK ON TRIPLETS (DATUM_ID)";
    static public String indexOnUserIdFkCreateQuery = "CREATE INDEX USER_ID_FK ON TRIPLETS (USER_ID)";
}
