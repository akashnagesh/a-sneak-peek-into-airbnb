package com.BigData.StayAccomodationMapReduce;
import com.BigData.utils.*;

public class App1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	        String headers = "id,listing_url,scrape_id,last_scraped,name,summary,space,description,experiences_offered,neighborhood_overview,notes,transit,access,interaction,house_rules,thumbnail_url,medium_url,picture_url,xl_picture_url,host_id,host_url,host_name,host_since,host_location,host_about,host_response_time,host_response_rate,host_acceptance_rate,host_is_superhost,host_thumbnail_url,host_picture_url,host_neighbourhood,host_listings_count,host_total_listings_count,host_verifications,host_has_profile_pic,host_identity_verified,street,neighbourhood,neighbourhood_cleansed,neighbourhood_group_cleansed,city,state,zipcode,market,smart_location,country_code,country,latitude,longitude,is_location_exact,property_type,room_type,accommodates,bathrooms,bedrooms,beds,bed_type,amenities,square_feet,price,weekly_price,monthly_price,security_deposit,cleaning_fee,guests_included,extra_people,minimum_nights,maximum_nights,calendar_updated,has_availability,availability_30,availability_60,availability_90,availability_365,calendar_last_scraped,number_of_reviews,first_review,last_review,review_scores_rating,review_scores_accuracy,review_scores_cleanliness,review_scores_checkin,review_scores_communication,review_scores_location,review_scores_value,requires_license,license,jurisdiction_names,instant_bookable,cancellation_policy,require_guest_profile_picture,require_guest_phone_verification,calculated_host_listings_count,reviews_per_month";
	    	System.out.println(headers.split(",").length);
	        int d =ColumnParser.getTheIndexOfTheColumn(headers.split(","),"room_type");
	        int f = ColumnParser.getTheIndexOfTheColumn(headers.split(","),"price");
	        String[] s =headers.split(",");
	        System.out.println("room " + d);
	        System.out.println("price " +f );
	        System.out.println(s[f]);
	        System.out.println(s[d]);
	        
	    
		
	}

}
