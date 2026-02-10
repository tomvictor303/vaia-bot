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
	{ name: 'amenities', description: 'Amenities - Features, facilities, or services offered to enhance the guest experience, with details such as key inclusions, availability, access rules, and property applicability.'},
	{ name: 'cleanliness_enhancements', description: 'Cleanliness enhancements - Specific improvements or additional measures to maintain a higher level of hygiene and sanitation' },
	{ name: 'food_beverage', description: 'Food & beverage - Dining, bar, café, and catering; cuisine, meals, and drinks; hours and service times; bar details; private events and catering; staff handling instructions; plus property-specific service routing and named partners when stated.' },  
	{
		name: 'guest_rooms',
		description: 'Guest Rooms - Room types and all details including inventory count pet types; renovation status; bed type; sleeps count / occupancy; special features; accessibility, pet, and non-smoking notes; key room amenities; plus, when stated, room physical specifications (e.g., size, layout, temperature controls), kitchen or kitchenette contents, bathroom details, and outdoor space type or floor placement.',
		capture_guide: 'For each room type, try to capture all stated details. Do not omit any details.',
		merge_guide: 'Do not compose or invent room type name or item names. Use the exact name as stated on the page. A single room type can have multiple names; mention all of those names in merged text.'
	},
	{ name: 'guest_services_front_desk', description: 'Guest Services / Front Desk - Bell/porter service, Concierge, Lost & found inquiries, Luggage storage, Wake-up calls' },
	{ name: 'housekeeping_laundry', description: 'Housekeeping / Laundry - Cleaning, room upkeep, linens, guest laundry, guest clothing care' },
	{ 
		name: 'local_area_information', 
		description: 'Local Area Information - Outside the hotel: all named attractions, businesses, venues, services, and amenities, plus all their details including distances, attraction context, activities, food options, local transportation access, tour operators, and seasonality.',
		capture_guide: 'Do not drop or replace explicitly named places, businesses, or properties with generic labels. Do not omit any details.',
		merge_guide: 'Do not drop or replace explicitly named places, businesses, or properties with generic labels. Do not omit any details.'
	},
	{ name: 'meeting_events', description: 'Meeting & events - Spaces, services, and resources for hosting meetings, conferences, banquets, weddings, and social gatherings' },
	{ name: 'on_property_convenience', description: 'On-property convenience – Practical, guest-facing services that make the stay more seamless, accessible, and comfortable, along with supporting details such as equipment, availability or seasonality, and basic usage guidance where applicable.' },
	{ name: 'parking_transportation', description: 'Parking & transportation - Services, instructions, and logistics related to guest vehicles, access to the property, and travel options to and from the hotel' },
	{ name: 'policies', description: 'Policies - Formal rules, procedures, or guidelines for stays, covering check-in/out, payments, restrictions, fees, eligibility, and stated exceptions.' },
	{ 
		name: 'recreation_fitness', 
		description: 'Recreation & fitness - Facilities, activities, and services that support leisure, wellness, and physical activity',
		capture_guide: 'Capture all Recreation & fitness content, including facilities, activities, and services that support leisure, wellness, and physical activity; summarizing is allowed, but do not omit any details.', 
	},
	{ name: 'safety_security', description: 'Safety & Security - Emergency procedures (fire exits, severe weather protocols, Safe deposit boxes or in-room safes, Security staff or surveillance)' },
	{ name: 'technology_business_services', description: 'Technology / Business Services - Business center computers, printing, fax, and copying, Wi-Fi details, Public computer access' },
	// Plus, "faq" field
	{
		name: 'faq',
		description: 'FAQ - Frequently Asked Questions',
		capture_guide: 'Only capture explicit Q&A pairs from sections explicitly labeled as FAQ (Frequently Asked Questions). **Keep original question and answer as is. Do not abstract or summarize FAQ questions and answers.**',
		merge_guide: '**Keep original question and answer as is. Do not abstract or summarize FAQ questions and answers.**'
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
