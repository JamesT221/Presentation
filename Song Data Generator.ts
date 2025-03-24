import { join } from "https://deno.land/std/path/mod.ts";
import { parse } from "https://deno.land/std@0.192.0/csv/mod.ts";
import mysql from "mysql2/promise";
import { writeToPath } from "fast-csv";

// Fetch song metadata from MusicBrainz API
async function fetchMusicBrainzMetadata(songName: string, artistName: string = "") {
  try {
    const query = encodeURIComponent(`${songName} ${artistName}`);
    const response = await fetch(`https://musicbrainz.org/ws/2/recording/?query=${query}&fmt=json`);

    if (!response.ok) {
      console.error(`Error fetching MusicBrainz metadata for '${songName}': ${response.statusText}`);
      return null;
    }

    const data = await response.json();
    const recording = data.recordings?.[0];

    if (!recording) {
      console.warn(`No MusicBrainz data found for '${songName}'.`);
      return null;
    }

    return {
      album: recording.releases?.[0]?.title || "Unknown Album",
      artist: recording["artist-credit"]?.[0]?.name || "Unknown Artist",
      release_date: recording.releases?.[0]?.date || "Unknown Release Date",
      duration: recording.length
        ? `${Math.floor(recording.length / 60000)}:${Math.floor((recording.length % 60000) / 1000)
            .toString()
            .padStart(2, "0")}`
        : "Unknown Duration",
    };
  } catch (error) {
    console.error("Error fetching metadata:", error);
    return null;
  }
}

export const db = mysql.createPool({
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  host: "database-1.cluster-c12mk4s0y8o5.us-east-2.rds.amazonaws.com",
  user: "admin",
 // password: "DrigU=vdott3",
  database: "database-1",
  connectTimeout: 30000,
});

async function createPlaylistTable() {
  const connection = await db.getConnection();
  try {
    const query = `
      CREATE TABLE IF NOT EXISTS playlists (
        id INT AUTO_INCREMENT PRIMARY KEY,
        playlist_name VARCHAR(255) NOT NULL,
        song_name VARCHAR(255) NOT NULL,
        song_url TEXT NOT NULL,
        album VARCHAR(255),
        artist VARCHAR(255),
        release_date VARCHAR(50),
        duration VARCHAR(50)
      );
    `;
    await connection.query(query);
    console.log("Created 'playlists' table if not existing.");
  } catch (err) {
    console.error("Error creating or altering table:", err);
  } finally {
    connection.release();
  }
}

async function populateDatabaseFromCSV(csvFilePath: string) {
  try {
    console.log(`Reading CSV file: ${csvFilePath}`);
    const content = await Deno.readTextFile(csvFilePath);

    const records = [];
    for await (const row of parse(content, {
      skipFirstRow: true,
      columns: ["playlist_name", "song_name", "song_url"],
    })) {
      records.push(row);
    }

    console.log(`Successfully read ${records.length} records from the CSV file.`);

    const connection = await db.getConnection();
    try {
      for (const record of records) {
        const { playlist_name, song_name, song_url } = record;

        const [existingRecord] = await connection.query(
          "SELECT * FROM playlists WHERE playlist_name = ? AND song_name = ?",
          [playlist_name, song_name]
        );

        if (existingRecord.length === 0) {
          // Fetch additional metadata from MusicBrainz
          const metadata = await fetchMusicBrainzMetadata(song_name);

          const query = `INSERT INTO playlists (playlist_name, song_name, song_url, album, artist, release_date, duration) VALUES (?, ?, ?, ?, ?, ?, ?)`;
          await connection.query(query, [
            playlist_name,
            song_name,
            song_url || "",
            metadata?.album || "Unknown",
            metadata?.artist || "Unknown",
            metadata?.release_date || "Unknown",
            metadata?.duration || "Unknown",
          ]);

          console.log(`Inserted record: Playlist = ${playlist_name}, Song = ${song_name}`);
        } else {
          console.log(`Song '${song_name}' already exists in playlist '${playlist_name}'. Skipping.`);
        }
      }
    } finally {
      connection.release();
    }
  } catch (err) {
    console.error("Error processing CSV file:", err);
  }
}

async function exportPlaylistToCSV(outputCsvFilePath: string) {
    let connection;
    try {
      connection = await db.getConnection();
      console.log("Connected to the database.");
  
      // Fetch all rows from the database
      const [rows] = await connection.query(
        "SELECT playlist_name, song_name, song_url, album, artist, release_date, duration FROM playlists"
      );
  
      console.log("Fetched rows from database:", rows);
  
      if (!rows || rows.length === 0) {
        console.log(`No records found in the database.`);
        return;
      }
  
      writeToPath(outputCsvFilePath, rows, { headers: true });
      console.log("File written successfully:", outputCsvFilePath);
    } catch (error) {
      console.error("Error exporting playlist:", error.stack || error);
    } finally {
      if (connection) connection.release();
    }
}
  
(async () => {
  const csvFilePath = join(Deno.cwd(), "playlistholder.csv");
  const outputCsvFilePath = join(Deno.cwd(), "output.csv");

  await createPlaylistTable();
  await populateDatabaseFromCSV(csvFilePath);

  const playlistName = "MyPlaylist";
  await exportPlaylistToCSV( outputCsvFilePath);
})()
