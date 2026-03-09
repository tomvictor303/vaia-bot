import { executeQuery } from '../config/database.js';

let config = {
    modelVersion: null, // e.g: gpt-4o, sonar-pro, etc.
    promptVersion: null // Decimal (10, 1). e.g: 1.0, 1.1, 1.2, etc.
}

async function init() {
    // We still do not use DB based config.
    // const rows = await executeQuery(
    //     'SELECT model_version, prompt_version FROM llm_config LIMIT 1',
    //     []
    // );
    // const row = rows[0];
    // if (row) {
    //     config.modelVersion = row.model_version;
    //     config.promptVersion = row.prompt_version;
    // }
    config.modelVersion = 'gpt-4o';
    config.promptVersion = '1.0';
}

export {
    init,
    config
}