// Centralized field definitions for market_data
// Only stable identifiers are id and hotel_uuid; others are defined here

// Primary/Principal fields (basic fields)
export const MD_PR_FIELDS = [
	{ name: 'name', capture_description: 'hotel name' },
	{ name: 'city_state_country', capture_description: 'city, state, country' },
	{ name: 'address', capture_description: 'street address' },
	{ name: 'zipcode', capture_description: 'zipcode or postal code' },
	{ name: 'description', capture_description: 'hotel description' },
	{ name: 'email', capture_description: 'contact email' },
	{ name: 'main_phone', capture_description: 'main phone number' },
	{ name: 'other_phones', capture_description: 'All phone numbers with descriptions. e.g: "Front Desk: (123) 456-7890"' },
];

// Category text fields (16 categories plus "other")
export const MD_CAT_FIELDS = [
	{ name: 'basic_information', capture_description: 'Basic Information - name, location, phone numbers, email, description, history, website.' },
	{ name: 'accessibility', capture_description: 'Accessibility - ADA-compliant rooms, Accessible entrances, restrooms, and elevators, Assistive devices or services' },
	{ name: 'amenities', capture_description: 'Amenities - Feature, facility, or service offered to enhance the guest experience' },
	{ name: 'cleanliness_enhancements', capture_description: 'Cleanliness enhancements - Specific improvements or additional measures to maintain a higher level of hygiene and sanitation' },
	{ name: 'food_beverage', capture_description: 'Food & beverage - Dining, bar, café, and catering services provided' },
	{ name: 'guest_rooms', capture_description: 'Guest Rooms - Guest rooms types' },
	{ name: 'guest_services_front_desk', capture_description: 'Guest Services / Front Desk - Bell/porter service, Concierge, Lost & found inquiries, Luggage storage, Wake-up calls' },
	{ name: 'housekeeping_laundry', capture_description: 'Housekeeping / Laundry - Cleaning, room upkeep, linens, guest laundry, guest clothing care' },
	{ name: 'local_area_information', capture_description: 'Local Area Information - Attractions, services, and amenities outside the hotel' },
	{ name: 'meeting_events', capture_description: 'Meeting & events - Spaces, services, and resources for hosting meetings, conferences, banquets, weddings, and social gatherings' },
	{ name: 'on_property_convenience', capture_description: 'On property convenience - Practical, guest-facing services that make the stay more seamless, accessible, and comfortable' },
	{ name: 'parking_transportation', capture_description: 'Parking & transportation - Services, instructions, and logistics related to guest vehicles, access to the property, and travel options to and from the hotel' },
	{ name: 'policies', capture_description: 'Policies - Formal set of guidelines, rules, or procedures' },
	{ name: 'recreation_fitness', capture_description: 'Recreation & fitness - Facilities, activities, and services that support leisure, wellness, and physical activity' },
	{ name: 'safety_security', capture_description: 'Safety & Security - Emergency procedures (fire exits, severe weather protocols, Safe deposit boxes or in-room safes, Security staff or surveillance)' },
	{ name: 'technology_business_services', capture_description: 'Technology / Business Services - Business center computers, printing, fax, and copying, Wi-Fi details, Public computer access' },
	// Plus, "other" field
	{ name: 'other', capture_description: 'Other - Any other information that is not covered by the other categories' },
];

// All fields - merge of primary and category fields
export const MD_ALL_FIELDS = [...MD_PR_FIELDS, ...MD_CAT_FIELDS];

// Boolean fields - none in simplified structure
export const BOOLEAN_FIELDS = [];
