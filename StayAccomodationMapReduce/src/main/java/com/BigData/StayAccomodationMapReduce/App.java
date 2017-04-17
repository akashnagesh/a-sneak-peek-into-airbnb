
package com.BigData.StayAccomodationMapReduce;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joni.Regex;

import com.BigData.utils.ColumnParser;
//import com.opencsv.CSVReader;


/**
 * Hello world!
 *
 *//*
public class App 
{
	
	

	
	static String headers = "id,listing_url,scrape_id,last_scraped,name,summary,space,description,experiences_offered,neighborhood_overview,notes,transit,access,interaction,house_rules,thumbnail_url,medium_url,picture_url,xl_picture_url,host_id,host_url,host_name,host_since,host_location,host_about,host_response_time,host_response_rate,host_acceptance_rate,host_is_superhost,host_thumbnail_url,host_picture_url,host_neighbourhood,host_listings_count,host_total_listings_count,host_verifications,host_has_profile_pic,host_identity_verified,street,neighbourhood,neighbourhood_cleansed,neighbourhood_group_cleansed,city,state,zipcode,market,smart_location,country_code,country,latitude,longitude,is_location_exact,property_type,room_type,accommodates,bathrooms,bedrooms,beds,bed_type,amenities,square_feet,price,weekly_price,monthly_price,security_deposit,cleaning_fee,guests_included,extra_people,minimum_nights,maximum_nights,calendar_updated,has_availability,availability_30,availability_60,availability_90,availability_365,calendar_last_scraped,number_of_reviews,first_review,last_review,review_scores_rating,review_scores_accuracy,review_scores_cleanliness,review_scores_checkin,review_scores_communication,review_scores_location,review_scores_value,requires_license,license,jurisdiction_names,instant_bookable,cancellation_policy,require_guest_profile_picture,require_guest_phone_verification,calculated_host_listings_count,reviews_per_month";
    
    
    
	/*public static void main(String[] args) {

        String csvFile = "/home/vinay/Documents/InsideAirBnb/Germany/listings.csv";

        CSVReader reader = null;
        int count = 0;
        try {
            reader = new CSVReader(new FileReader(csvFile),',','"');
            String[] line;
            while ((line = reader.readNext()) != null) {
               // System.out.println("Country [id= " + line[0] + ", code= " + line[1] + " , name=" + line[2] + "]");
            	System.out.println(line.length);
            	if(line.length==95){
            		count++;
            	}
            	//System.out.println(" price "  +line[60]);
            }
            System.out.println(" Count " +count);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }*/
	
	
	

	import java.io.BufferedReader;
	import java.io.FileReader;
	import java.io.IOException;
	import java.util.ArrayList;

	public class App {
		/** characters used as delimiters */
		private char[] separators = {',', '\t'};
		
		/** when the delimiters appears in the text the value will be between two double quotas */
		private char specialChars = '"';
		
		/**
		 * Method used to spit each line into values
		 * 
		 * @param line
		 * @return the array of values
		 */
		private String[] lineParser(String line) {
			String[] result = null;
			
			/** Using ArrayList as the number of values are unknown at this stage */
			ArrayList<String> parsedLine = new ArrayList<String>();
			
			int len = line.length();
			int i = 0;
			
			/** iterate through all the chars in the line */
			while (i < len) {
				int nextSep = len;
				/** Get the next separator */
				for (int j = 0; j < separators.length; ++j) {
					int temp = line.indexOf(separators[j], i);
					if ((temp == -1) || (temp >= nextSep))
						continue;
					nextSep = temp;
				}
				
				/** Place the special separator at the end of the string */
				int nextSpecialSep = len;
				
				/** Check if there is any special separator */
				int temp = line.indexOf(specialChars, i);
				if ((temp == -1) || (temp >= nextSpecialSep))
					nextSpecialSep = len;
				else
					nextSpecialSep = temp;
				
				/** if we are at the special separator get the text until the next special separator */
				if (nextSpecialSep == i) {
					char c = line.charAt(i);
					/** check if there is any double quote chars in the text */
					int d = line.indexOf((c + "") + (c + ""), i + 1);
					
					/** if there are two double quota chars jump to the next one - are part of the text */
					int end = line.indexOf(c, d >= 0 ? d + 3 : i + 1);
					if (end == -1) {
						end = len;
					}
					String toAdd = line.substring(i + 1, end);
					/** Replace two double quota with one double quota */
					toAdd = toAdd.replaceAll((c + "") + (c + ""), c + "");
					
					parsedLine.add(toAdd);
					i = end + 1;
				}
				/** if we are at a normal separator, ignore the separator and jump to the next char */
				else if (nextSep == i) {
					++i;
				}
				/** Copy the value in the result string */
				else {
					parsedLine.add(line.substring(i, nextSep));
					i = nextSep;
				}
			}
			
			/** Convert the result to String[] */
			result = parsedLine.toArray(new String[parsedLine.size()]);
			return result;
		}
		
		/**
		 * 
		 * Method used to parse the file
		 * 
		 * @param path
		 *           to the file
		 * @return array of all lines
		 */
		public ArrayList<String[]> parser(String path) {
			BufferedReader br = null;
			ArrayList<String[]> result = new ArrayList<String[]>();
			try {
				
				br = new BufferedReader(new FileReader(path));
				
				/** Parsing each line in the file */
				String line = "";
				while ((line = br.readLine()) != null) {
					
					/** Parse each line into values */
					String[] values = lineParser(line);
					
					/** Adding the lines to the array list */
					result.add(values);
				}
			}
			catch (Exception e) {
				/** Just display the error */
				e.printStackTrace();
			}
			finally {
				/** Closing the the stream */
				if (br != null) {
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			return result;
		}
		
		/**
		 * main method for testing
		 * 
		 * @param args
		 */
		public static void main(String[] args)	{
			String path = "/home/vinay/Documents/InsideAirBnb/Germany/listings.csv";
			System.out.println("CSV Parser Example");
			System.out.println("Parsing file " + path);
			App parser = new App();
			ArrayList<String[]> lines = parser.parser(path);
			
			System.out.println("File Content");
			for (int i = 0; i < lines.size(); i++) {
				String[] line = lines.get(i);
				System.out.println(line.length);
				/*for (int j = 0; j < line.length; j++) {
					String print = String.format("%-45s", line[j]);
					System.out.print(print);
				}*/
				System.out.println();
			}
		}
	}
    

	