package com.cassandraapps.flightsqueryapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TimeZone;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

public class BulkLoadFlightsByAirtime {

	public static final String DEFAULT_OUTPUT_DIR = "./data";
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd");
	public static final String KEYSPACE = "flight_details";
	public static final String TABLE = "flights_byairtime";

	private static Map<String, String> timeZones = new HashMap<String, String>();
			
	public static final String SCHEMA = String.format("CREATE TABLE %s.%s ("
			+"\"ID\" int, \"CARRIER\" varchar, \"ORIGIN_AIRPORT_ID\" int, "
			+"\"ORIGIN\" varchar, \"DEST\" varchar, \"DEP_TIME\" timestamp,\"AIR_TIME\" timestamp, \"TEN_MIN_TIME\" timestamp, "
			+ "PRIMARY KEY (\"ID\",\"TEN_MIN_TIME\") )", KEYSPACE, TABLE);

	/**
	 * INSERT statement to bulk load. It is like prepared statement. You fill in
	 * place holder for each data.
	 */
	public static final String INSERT_STMT = String.format(
			"INSERT INTO %s.%s ("
					+"\"ID\" , \"CARRIER\" , \"ORIGIN_AIRPORT_ID\" , "
					+"\"ORIGIN\" , \"DEST\" , \"DEP_TIME\" ,\"AIR_TIME\", \"TEN_MIN_TIME\"  "
					+ ") VALUES ("
					+ "?, ?, ?, ?, ?, ?, ?, ?" + ")",
			KEYSPACE, TABLE);

	public static void main(String[] args) throws Exception {
		readTimeZone();
		// magic!
		Config.setClientMode(true);

		// Create output directory that has keyspace and table name in the path
		File outputDir = new File(DEFAULT_OUTPUT_DIR + File.separator
				+ KEYSPACE + File.separator + TABLE);
		if (!outputDir.exists() && !outputDir.mkdirs()) {
			throw new RuntimeException("Cannot create output directory: "
					+ outputDir);
		}

		// Prepare SSTable writer
		CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
		// set output directory
		builder.inDirectory(outputDir)
		// set target schema
				.forTable(SCHEMA)
				// set CQL statement to put data
				.using(INSERT_STMT)
				// set partitioner if needed
				// default is Murmur3Partitioner so set if you use different
				// one.
				.withPartitioner(new Murmur3Partitioner());
		CQLSSTableWriter writer = builder.build();

		InputStream in = BulkLoadFlightsTable.class
				.getResourceAsStream("/flights_from_pg.csv");

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(
				in));
				CsvListReader csvReader = new CsvListReader(reader,
						CsvPreference.STANDARD_PREFERENCE)) {
			csvReader.getHeader(true);

			// Write to SSTable while reading data
			List<String> line;
			while ((line = csvReader.read()) != null) {
				
				String id = line.get(0);
				String year = line.get(1);
				String day_of_month = line.get(2);

				String fl_date = line.get(3);
				fl_date = fl_date.replace('/', '-');				

				String airline_id = line.get(4);
				String carrier = line.get(5);
				String fl_num = line.get(6);
				String origin_airport_id = line.get(7);
				String origin = line.get(8);
				String origin_city_name = line.get(9);
				String origin_state_abr = line.get(10);
				String dest = line.get(11);
				String dest_city_name = line.get(12);
				String dest_state_abr = line.get(13);
				
				String dep_time = line.get(14);
				dep_time = formatTime(fl_date, dep_time);
				Date dep_time_dt = findTimeBasedOnTimeZone(dep_time,origin);
				
				String arr_time = line.get(15);
				arr_time = formatTime(fl_date, arr_time);
				Date arr_time_dt = findTimeBasedOnTimeZone(arr_time,dest);

				String actual_elapsed_time = line.get(16);
				String air_time = line.get(17);
				String distance = line.get(18);
				
				Date air_end_timeindtobj = addMinutesToTime(dep_time_dt,Integer.parseInt(air_time));
				
				fl_date = formatTime(fl_date, "00");				
				Date fl_date_dt = findTimeBasedOnTimeZone(fl_date,origin);

				Date tenMinDt = addMinutesToTime(dep_time_dt,0);
				writer.addRow(Integer.parseInt(id),
						carrier,						
						Integer.parseInt(origin_airport_id),
						origin,
						dest,						
						dep_time_dt,
						air_end_timeindtobj,
						tenMinDt);
				
				
				int airTime = Integer.parseInt(air_time);
				int count = 1;
				while((airTime - 10) >= 0) {
					airTime = airTime - 10;
					tenMinDt = addMinutesToTime(dep_time_dt,10*count);
					writer.addRow(Integer.parseInt(id),
							carrier,						
							Integer.parseInt(origin_airport_id),
							origin,
							dest,						
							dep_time_dt,
							air_end_timeindtobj,
							tenMinDt);					
					count++;
				}
				if(airTime-10<0) {
					tenMinDt = addMinutesToTime(dep_time_dt,(airTime)+(10*(count-1)));
					writer.addRow(Integer.parseInt(id),
							carrier,						
							Integer.parseInt(origin_airport_id),
							origin,
							dest,						
							dep_time_dt,
							air_end_timeindtobj,
							tenMinDt);					
				}
				
			}
		} catch (InvalidRequestException | IOException e) {
			e.printStackTrace();
		}

		try {
			writer.close();
		} catch (IOException ignore) {
		}
	}
	
	private static Date addMinutesToTime(Date time, int min) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(time);
		cal.add(Calendar.MINUTE, min);

		return cal.getTime();
	}
	
	private static String formatTime(String date, String time) {
		String timeStr = "";
		if (time.length() == 3) {
			timeStr = time.substring(0, 1) + ":" + time.substring(1, 3);
		} else if (time.length() == 4) {
			if (Integer.parseInt(time) == 2400)
				timeStr = "23:59";
			else
				timeStr = time.substring(0, 2) + ":" + time.substring(2, 4);
		} else if (time.length() == 2) {
			timeStr = "00:" + time;
		} else {
			return date;
		}
		return date + " " + timeStr;
	}
	
	private static Date findTimeBasedOnTimeZone(String time, String airport) {
		if(time.trim().length()<11)
			time = time+" 00:00";
		String timeZoneStr = timeZones.get(airport);
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm zzzz");
		//format.setTimeZone(TimeZone.getTimeZone(timeZoneStr).);
		Date date=null;
		try {
			date = format.parse(time+" "+TimeZone.getTimeZone(timeZoneStr).getDisplayName());
		} catch (ParseException e) {
			e.printStackTrace();
			System.exit(0);
		}
				
		//format = new SimpleDateFormat("yyyy-MM-dd HH:mmZ");
		//format.setTimeZone(TimeZone.getTimeZone(timeZoneStr)); 
		//String formattedDate = format.format(date);
		//return formattedDate;
		return date;
	}
	
	private static void readTimeZone() throws IOException {
		InputStream in = BulkLoadFlightsTable.class.getResourceAsStream("/iata.tzmap.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line;	
		while ((line = br.readLine()) != null) {
			StringTokenizer stringTkns = new StringTokenizer(line);
			timeZones.put(stringTkns.nextToken(), stringTkns.nextToken());
		}		
	}
	
}
