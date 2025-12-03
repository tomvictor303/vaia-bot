import OpenAI from "openai";
import extractJson from 'extract-json-from-string';
import { MD_ALL_FIELDS } from '../middleware/constants.js';

const openai = new OpenAI({
  apiKey: process.env["PERPLEXITY_API_KEY"],
  baseURL: "https://api.perplexity.ai",
});

export class AIService {
  
  // Fetch hotel data using AI
  // @param {string} hotelName - Name of the hotel to fetch data for
  // @param {Array<string>} fieldsToFetch - Optional: specific fields to fetch (if empty, fetches all)
  static async fetchHotelData(hotelName, fieldsToFetch = null) {
    console.log(`🔍 Fetching data for: ${hotelName}`);
    
    // Filter fields if specific fields requested
    const fields = fieldsToFetch 
      ? MD_ALL_FIELDS.filter(f => fieldsToFetch.includes(f.name))
      : MD_ALL_FIELDS;
    
    if (fields.length === 0) {
      throw new Error("No fields to fetch");
    }
    
    const fieldsDoc = fields
      .map(f => `  "${f.name}": "${f.capture_description}"`)
      .join(',\n');

    const fieldsNote = fieldsToFetch 
      ? `\n\nIMPORTANT: Only return the fields listed above. Focus on finding these specific pieces of information.`
      : '';

    const prompt = `Parse live info from this hotel's website or any other reliable sources:

"${hotelName}"

Return a JSON object with EXACTLY these key-value pairs (single level, no nested objects):

{
${fieldsDoc}
}
${fieldsNote}

If any information is not available, use just "N/A" as the value, no quotes.
Please do **deep** live web search to get current information. Do not make up any information. Do not return any other text than the JSON object.`;

    try {
      const completions = await openai.chat.completions.create({
        model: "sonar-pro",
        messages: [{ role: "user", content: prompt }],
        max_tokens: 1024 * 10 * 4,
        stream: true,
      });

      // Collect the full response
      let fullResponse = "";
      for await (const part of completions) {
        const content = part.choices[0]?.delta?.content || "";
        fullResponse += content;
        process.stdout.write(content);
      }

      console.log("\n" + "=".repeat(50));
      console.log("EXTRACTING JSON...");
      console.log("=".repeat(50));

      // Extract JSON using extract-json-from-string
      const extractedJsonObjects = extractJson(fullResponse);
      
      if (extractedJsonObjects.length === 0) {
        throw new Error("No JSON found in response");
      }
      
      // Use the first (and usually only) JSON object found
      const parsedJson = extractedJsonObjects[0];

      // Validate requested fields
      const requestedFields = fields.map(f => f.name);
      const missingFields = requestedFields.filter(field => !(field in parsedJson));
      
      if (missingFields.length > 0) {
        console.log(`⚠️  Missing fields in response: ${missingFields.join(', ')}`);
      } else {
        console.log(`✅ All requested fields (${requestedFields.length}) present in response!`);
      }

      return parsedJson;

    } catch (error) {
      console.error(`❌ Error fetching data for ${hotelName}:`, error.message);
      throw error;
    }
  }

  /**
   * Fetch hotel FAQs (question/answer pairs) using AI
   * @param {string} hotelName - Name of the hotel to fetch FAQs for
   * @returns {Promise<Array<{question: string, answer: string}>>}
   */
  static async fetchHotelFAQ(hotelName) {
    console.log(`\n📚 Fetching FAQs for: ${hotelName}`);

    const prompt = `Parse FAQ content from this hotel's official FAQ page:

"${hotelName}"

Return a JSON array of objects with EXACTLY this structure:
[
  {
    "question": "Question text?",
    "answer": "Answer text."
  }
]

Rules:
- Provide all Q/A pairs. Parse the FAQ page carefully and provide all the questions and answers.
- If FAQ content is unavailable, return an empty array [].
- Do not make up any information.
- Do not include any text outside the JSON array.`;

    try {
      const completions = await openai.chat.completions.create({
        model: "sonar-pro",
        messages: [{ role: "user", content: prompt }],
        max_tokens: 1024 * 10 * 8,
        stream: true,
      });

      let fullResponse = "";
      for await (const part of completions) {
        const content = part.choices[0]?.delta?.content || "";
        fullResponse += content;
      }

      const extractedJsonObjects = extractJson(fullResponse);
      if (extractedJsonObjects.length === 0) {
        console.log("⚠️  No FAQ JSON found in response");
        return [];
      }

      const faqs = extractedJsonObjects[0];
      if (!Array.isArray(faqs)) {
        console.log("⚠️  FAQ response was not an array");
        return [];
      }

      return faqs;
    } catch (error) {
      console.error(`❌ Error fetching FAQs for ${hotelName}:`, error.message);
      throw error;
    }
  }
}
