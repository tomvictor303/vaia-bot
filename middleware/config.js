// import { executeQuery } from '../config/database.js';
// DATABASE configuration will not be included here.
// We also **mostly rely on .ENV file** for configuration.
// This is flexible layer to provide same configuration interface from different configuration sources.

let config = {
    modelVersion: null, // e.g: gpt-4o, sonar-pro, etc.
    promptVersion: null // Decimal (10, 1). e.g: 1.0, 1.1, 1.2, etc.
}

async function initConfig() {
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
    // Currently, we rely on static config.
    config.modelVersion = 'sonar-pro';
    config.promptVersion = '1.0';
}

export {
    initConfig,
    config
}