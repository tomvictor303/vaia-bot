import extractJson from 'extract-json-from-string';

/**
 * Parse LLM output into JSON using extract-json-from-string with fallbacks.
 * Returns {} if parsing fails.
 */
export function llmOutputToJson(raw) {
  if (!raw || typeof raw !== 'string') return {};

  const candidates = extractJson(raw);
  if (candidates && candidates.length > 0 && typeof candidates[0] === 'object') {
    return candidates[0];
  }

  try {
    const cleaned = raw.replace(/```json/gi, '').replace(/```/g, '').trim();
    return JSON.parse(cleaned);
  } catch {
    return {};
  }
}

