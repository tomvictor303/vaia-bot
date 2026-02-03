// Centralized field definitions for market_data
// Only stable identifiers are id and hotel_uuid; others are defined here

// Deprecated: Primary/Principal fields (basic fields)
const MD_PR_FIELDS = [
	{ name: 'name', description: 'hotel name' },
	{ name: 'city_state_country', description: 'city, state, country' },
	{ name: 'address', description: 'street address' },
	{ name: 'zipcode', description: 'zipcode or postal code' },
	{ name: 'description', description: 'hotel description' },
	{ name: 'email', description: 'contact email' },
	{ name: 'main_phone', description: 'main phone number' },
	{ name: 'other_phones', description: 'All phone numbers with descriptions. e.g: "Front Desk: (123) 456-7890"' },
];

// Category text fields (16 categories plus "other")
export const MD_CAT_FIELDS = [
	{ name: 'basic_information', description: 'Basic Information (of [hotelName]) - name, description, history, location (city, state, country, zipcode, street address)' },
	{ name: 'contacts', description: 'Contacts (of [hotelName]) - phone numbers (with descriptions), email addresses (with descriptions) and other contact channels (with descriptions)' },
	{ name: 'accessibility', description: 'Accessibility - ADA-compliant rooms, Accessible entrances, restrooms, and elevators, Assistive devices or services' },
	{ name: 'amenities', description: 'Amenities - Feature, facility, or service offered to enhance the guest experience' },
	{ name: 'cleanliness_enhancements', description: 'Cleanliness enhancements - Specific improvements or additional measures to maintain a higher level of hygiene and sanitation' },
	{ name: 'food_beverage', description: 'Food & beverage (Dining, bar, café, and catering services provided; plus cuisine, meals, and drinks served)' },
	{ 
		name: 'guest_rooms', 
		description: 'Guest Rooms - Guest rooms types',
		extract_guide: 'Extract all Guest Room Types content (room types and what differentiates them, plus key room amenities and accessibility/pet/non-smoking notes); summarizing is allowed, but do not omit.' 
	},
	{ name: 'guest_services_front_desk', description: 'Guest Services / Front Desk - Bell/porter service, Concierge, Lost & found inquiries, Luggage storage, Wake-up calls' },
	{ name: 'housekeeping_laundry', description: 'Housekeeping / Laundry - Cleaning, room upkeep, linens, guest laundry, guest clothing care' },
	{ name: 'local_area_information', description: 'Local Area Information - Attractions, services, and amenities outside the hotel' },
	{ name: 'meeting_events', description: 'Meeting & events - Spaces, services, and resources for hosting meetings, conferences, banquets, weddings, and social gatherings' },
	{ name: 'on_property_convenience', description: 'On property convenience - Practical, guest-facing services that make the stay more seamless, accessible, and comfortable' },
	{ name: 'parking_transportation', description: 'Parking & transportation - Services, instructions, and logistics related to guest vehicles, access to the property, and travel options to and from the hotel' },
	{ name: 'policies', description: 'Policies - Formal set of guidelines, rules, or procedures' },
	{ 
		name: 'recreation_fitness', 
		description: 'Recreation & fitness - Facilities, activities, and services that support leisure, wellness, and physical activity',
		extract_guide: 'Extract all Recreation & fitness content, including facilities, activities, and services that support leisure, wellness, and physical activity; summarizing is allowed, but do not omit any details.' 
	},
	{ name: 'safety_security', description: 'Safety & Security - Emergency procedures (fire exits, severe weather protocols, Safe deposit boxes or in-room safes, Security staff or surveillance)' },
	{ name: 'technology_business_services', description: 'Technology / Business Services - Business center computers, printing, fax, and copying, Wi-Fi details, Public computer access' },
	// Plus, "faq" field
	{
		name: 'faq',
		description: 'FAQ - Frequently Asked Questions',
		extract_guide: 'Only capture explicit Q&A pairs from sections explicitly labeled as FAQ (Frequently Asked Questions). Keep original question and answer as is'
	},
	// Plus, "other" field
	{ name: 'other', description: 'Other - Any other information that is not covered by the other categories' },
];

// All fields with meaningful data - originally merge of primary and category fields but now just category fields
export const MD_DATA_FIELDS = [...MD_CAT_FIELDS];

// Extra fields for data manipulation - not meaningful data fields. Supportive or metadata fields.
// In most cases, these fields are not needed to be fetched from the LLM.
// They are calculated from the meaningful data fields. i.e. other_structured is calculated from the other field.
// Or automatically generated.
export const MD_EXTRA_FIELDS = [
	{ name: 'other_structured', description: 'Other - Any other information that is not covered by the other categories' },
];

// All fields - merge of meaningful data fields and extra fields
export const MD_ALL_FIELDS = [...MD_DATA_FIELDS, ...MD_EXTRA_FIELDS];

// (Deprecated) Boolean fields - array of field names that are boolean values. 
export const BOOLEAN_FIELDS = [];
