#!/bin/bash

# Example cron usage:
# 43 21 * * * /home/vaia-bot/run_cron.sh
# This runs the script daily at 21:43 (9:43 PM)

# Define base directory
APP_DIR="/home/vaia-bot"

# Navigate to the app directory. This is **essential** for the script to load .env variables correctly.
cd "$APP_DIR" || exit 1   # Exit if cd fails

# NVM setup. This is essential for the script to use the correct Node version.
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"

# Use default Node version
nvm use default

# Run Node script
node index.js >> "$APP_DIR/cron.log" 2>&1
