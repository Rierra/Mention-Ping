# Bot Data Persistence on Render

## Problem
When you redeploy your bot on Render, the filesystem is ephemeral - any files (like `bot_data.json`) are lost on redeploy. This means you have to re-add all your channels, keywords, subreddits, etc. every time.

## Solution
This bot now supports **environment variable-based persistence** for your data. Your bot configuration (groups, keywords, subreddits, Slack workspaces) can be stored in the `BOT_DATA_JSON` environment variable, which persists across Render deployments.

## How It Works

1. **Loading Priority**: The bot loads data in this order:
   - First: `BOT_DATA_JSON` environment variable (if set)
   - Second: `bot_data.json` file (for local development)

2. **Saving**: The bot saves to:
   - File: `data/bot_data.json` (for backup and local dev)
   - Export file: `data/bot_data_env_export.txt` (for manual env var updates)

## Setup Instructions

### Initial Setup (First Time)

1. Set up your bot on Render as usual with your environment variables (Telegram token, Reddit credentials, etc.)

2. **After your first deployment**, configure your bot by adding groups, keywords, etc. through Telegram

3. **Export your data** by running `/exportdata` in your Telegram control group

4. **Copy the JSON** from the bot's response

5. **Go to Render Dashboard** → Your Service → Environment

6. **Add a new environment variable**:
   - Key: `BOT_DATA_JSON`
   - Value: Paste the entire JSON string from step 4

7. **Save** and **redeploy** your service

### Updating Data After Changes

Whenever you add/remove groups, keywords, or subreddits:

1. Run `/exportdata` in Telegram to get the latest JSON
2. Update the `BOT_DATA_JSON` environment variable in Render
3. Redeploy (or Render will auto-deploy if you have auto-deploy enabled)

### Optional: Using Persistent Disk (Alternative)

If you prefer to use Render's persistent disk storage instead:

1. Set up a persistent disk volume in Render
2. Set the `DATA_DIR` environment variable to the mounted path (e.g., `/opt/render/project/src/data`)
3. The bot will automatically use that directory for data storage

## Commands

- `/exportdata` - Export current bot data as JSON for environment variable backup (Owner only)

## Files

- `data/bot_data.json` - Full formatted JSON backup (local/dev)
- `data/bot_data_env_export.txt` - Compact JSON for environment variable

## Notes

- The `BOT_DATA_JSON` environment variable stores compact JSON (minified)
- Maximum size: Render environment variables have limits (~64KB typically)
- If your data exceeds limits, consider using Render's persistent disk instead
- The export file is automatically created whenever data is saved

## Troubleshooting

**Data not persisting?**
- Make sure `BOT_DATA_JSON` environment variable is set in Render
- Check the variable value is valid JSON (no newlines or formatting)
- Verify you saved and redeployed after setting the variable

**Export file too large?**
- If the JSON exceeds Telegram's message limit, it will be split into multiple messages
- Very large datasets might exceed Render's env var limit - consider using persistent disk

**Want to start fresh?**
- Delete the `BOT_DATA_JSON` environment variable
- Redeploy
- The bot will start with a fresh configuration

