#!/usr/bin/env python3
"""
Reddit to Telegram Monitor Bot
Monitors all of Reddit for specific keywords and sends notifications with content and links
"""

import os
import json
import time
import logging
import asyncio
import re
import aiohttp
import html
from typing import Set, List, Dict
from datetime import datetime, timedelta

import asyncpraw
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class RedditTelegramBot:
    def __init__(self):
        # Load configuration from environment variables
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        self.reddit_client_id = os.getenv('REDDIT_CLIENT_ID')
        self.reddit_client_secret = os.getenv('REDDIT_CLIENT_SECRET')
        self.reddit_user_agent = os.getenv('REDDIT_USER_AGENT', 'TelegramBot:v1.0')
        
        # Optional Reddit credentials for higher rate limits
        self.reddit_username = os.getenv('REDDIT_USERNAME', '')
        self.reddit_password = os.getenv('REDDIT_PASSWORD', '')
        
        # Configuration
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '300'))  # seconds
        self.max_posts = int(os.getenv('MAX_POSTS', '100'))
        
        # Data storage
        self.keywords: Set[str] = set()
        self.processed_posts: Set[str] = set()
        self.data_file = 'bot_data.json'
        
        # Rate limiting for notifications
        self.notification_delay = 5  # seconds between notifications
        self.pending_notifications = []
        
        # Initialize HTTP session for Telegram
        self.session = None
        
        # Reddit client will be initialized in setup_reddit
        self.reddit = None
        
        self.load_data()
        
    async def setup_reddit(self):
        """Initialize Reddit API client"""
        try:
            # Create aiohttp session for asyncpraw
            if not self.session:
                timeout = aiohttp.ClientTimeout(total=30, connect=10)
                self.session = aiohttp.ClientSession(timeout=timeout)
            
            if self.reddit_username and self.reddit_password:
                # Authenticated instance (higher rate limits)
                self.reddit = asyncpraw.Reddit(
                    client_id=self.reddit_client_id,
                    client_secret=self.reddit_client_secret,
                    user_agent=self.reddit_user_agent,
                    username=self.reddit_username,
                    password=self.reddit_password,
                    requestor_kwargs={'session': self.session}
                )
            else:
                # Read-only instance
                self.reddit = asyncpraw.Reddit(
                    client_id=self.reddit_client_id,
                    client_secret=self.reddit_client_secret,
                    user_agent=self.reddit_user_agent,
                    requestor_kwargs={'session': self.session}
                )
            
            logger.info("Reddit API initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Reddit API: {e}")
            raise
    
    def load_data(self):
        """Load keywords and processed posts from file"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r') as f:
                    data = json.load(f)
                    self.keywords = set(data.get('keywords', []))
                    self.processed_posts = set(data.get('processed_posts', []))
                    logger.info(f"Loaded {len(self.keywords)} keywords and {len(self.processed_posts)} processed posts")
        except Exception as e:
            logger.error(f"Error loading data: {e}")
    
    def save_data(self):
        """Save keywords and processed posts to file"""
        try:
            # Limit processed posts to last 10000 to prevent file from growing too large
            if len(self.processed_posts) > 10000:
                self.processed_posts = set(list(self.processed_posts)[-5000:])
                
            data = {
                'keywords': list(self.keywords),
                'processed_posts': list(self.processed_posts)
            }
            with open(self.data_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving data: {e}")
    
    def contains_keyword(self, text: str) -> List[str]:
        """Check if text contains any monitored keywords"""
        if not text:
            return []
        
        text_lower = text.lower()
        found_keywords = []
        
        for keyword in self.keywords:
            if keyword in text_lower:
                found_keywords.append(keyword)
        
        return found_keywords
    
    def clean_text_for_telegram(self, text: str) -> str:
        """Clean text for Telegram by removing/escaping problematic characters"""
        if not text:
            return ""
        
        # Unescape HTML entities first
        text = html.unescape(text)
        
        # Remove markdown formatting that might cause issues
        text = re.sub(r'\*{1,2}([^*]+)\*{1,2}', r'\1', text)  # Remove bold/italic
        text = re.sub(r'_{1,2}([^_]+)_{1,2}', r'\1', text)    # Remove underline
        text = re.sub(r'`([^`]+)`', r'\1', text)              # Remove code formatting
        text = re.sub(r'~~([^~]+)~~', r'\1', text)            # Remove strikethrough
        
        # Remove or replace other problematic characters
        text = re.sub(r'[<>]', '', text)  # Remove angle brackets
        text = re.sub(r'&[a-zA-Z0-9#]+;', '', text)  # Remove remaining HTML entities
        
        # Clean up extra whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)  # Max 2 consecutive newlines
        text = re.sub(r' {2,}', ' ', text)      # Remove multiple spaces
        
        return text.strip()
    
    def format_notification(self, item, found_keywords: List[str], item_type: str) -> str:
        """Format notification message"""
        keywords_str = ", ".join(found_keywords)
        
        try:
            if item_type == "post":
                title = item.title[:200] + "..." if len(item.title) > 200 else item.title
                content = item.selftext[:500] + "..." if len(item.selftext) > 500 else item.selftext
                
                message = f"Keyword match: {keywords_str}\n\n"
                message += f"Post: {title}\n"
                message += f"By: u/{item.author}\n"
                message += f"Subreddit: r/{item.subreddit}\n"
                
                if content and content.strip():
                    message += f"\nContent:\n{content}\n"
                
                message += f"\nLink: https://reddit.com{item.permalink}"
                
            else:  # comment
                content = item.body[:500] + "..." if len(item.body) > 500 else item.body
                
                message = f"Keyword match: {keywords_str}\n\n"
                message += f"Comment by: u/{item.author}\n"
                message += f"Subreddit: r/{item.subreddit}\n"
                message += f"\nComment:\n{content}\n"
                message += f"\nLink: https://reddit.com{item.permalink}"
                
        except AttributeError as e:
            logger.error(f"Error formatting notification: {e}")
            message = f"Keyword match: {keywords_str}\n\nError formatting item details."
        
        return message
    
    async def send_notification(self, message: str):
        """Queue notification to be sent with rate limiting"""
        self.pending_notifications.append(message)
        logger.info(f"Queued notification, {len(self.pending_notifications)} in queue")
    
    async def process_notifications(self):
        """Process queued notifications with rate limiting"""
        while self.pending_notifications:
            try:
                message = self.pending_notifications.pop(0)
                await self._send_telegram_message(message)
                logger.info("Notification sent successfully")
                
                # Wait before sending next notification
                if self.pending_notifications:  # Only wait if there are more to send
                    await asyncio.sleep(self.notification_delay)
                    
            except Exception as e:
                logger.error(f"Error processing notification: {e}")
                # Re-add the message to the front of the queue to retry later
                self.pending_notifications.insert(0, message)
                await asyncio.sleep(self.notification_delay * 2)  # Wait longer on error
                break  # Exit loop to prevent infinite retry in same cycle
    
    async def _send_telegram_message(self, message: str):
        """Send a single message to Telegram"""
        try:
            if not self.session:
                timeout = aiohttp.ClientTimeout(total=30)
                self.session = aiohttp.ClientSession(timeout=timeout)
                
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            data = {
                'chat_id': self.telegram_chat_id,
                'text': message,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            }
            
            async with self.session.post(url, data=data) as response:
                if response.status == 429:  # Rate limited
                    response_json = await response.json()
                    retry_after = response_json.get('parameters', {}).get('retry_after', 60)
                    logger.warning(f"Rate limited, waiting {retry_after} seconds")
                    await asyncio.sleep(retry_after)
                    # Retry the request
                    async with self.session.post(url, data=data) as retry_response:
                        if retry_response.status != 200:
                            response_text = await retry_response.text()
                            logger.error(f"Failed to send notification after retry: {response_text}")
                            raise Exception(f"Telegram API error: {response_text}")
                elif response.status != 200:
                    response_text = await response.text()
                    logger.error(f"Failed to send notification: {response_text}")
                    raise Exception(f"Telegram API error: {response_text}")
                    
        except Exception as e:
            logger.error(f"Error sending notification: {e}")
            raise
    
    async def monitor_reddit(self):
        """Monitor Reddit for keyword matches"""
        if not self.keywords:
            logger.info("No keywords to monitor")
            return
        
        try:
            if not self.reddit:
                await self.setup_reddit()
                
            logger.info("Checking Reddit for keyword matches...")
            
            # Monitor new posts from r/all with error handling
            try:
                subreddit = await self.reddit.subreddit('all')
                post_count = 0
                
                async for post in subreddit.new(limit=self.max_posts):
                    try:
                        if post.id in self.processed_posts:
                            continue
                        
                        post_count += 1
                        
                        # Check post title and content
                        found_keywords = []
                        found_keywords.extend(self.contains_keyword(post.title))
                        found_keywords.extend(self.contains_keyword(post.selftext))
                        
                        if found_keywords:
                            message = self.format_notification(post, list(set(found_keywords)), "post")
                            await self.send_notification(message)
                            logger.info(f"Queued notification for post: {post.id}")
                        
                        self.processed_posts.add(post.id)
                        
                        # Small delay to avoid rate limiting
                        await asyncio.sleep(0.1)
                        
                    except Exception as e:
                        logger.error(f"Error processing post: {e}")
                        continue
                
                logger.info(f"Processed {post_count} posts")
                
            except Exception as e:
                logger.error(f"Error fetching posts: {e}")
            
            # Monitor new comments from r/all with error handling
            try:
                comment_count = 0
                
                async for comment in subreddit.comments(limit=self.max_posts):
                    try:
                        if comment.id in self.processed_posts:
                            continue
                        
                        comment_count += 1
                        
                        # Check comment content
                        found_keywords = self.contains_keyword(comment.body)
                        
                        if found_keywords:
                            message = self.format_notification(comment, found_keywords, "comment")
                            await self.send_notification(message)
                            logger.info(f"Queued notification for comment: {comment.id}")
                        
                        self.processed_posts.add(comment.id)
                        
                        # Small delay to avoid rate limiting
                        await asyncio.sleep(0.1)
                        
                    except Exception as e:
                        logger.error(f"Error processing comment: {e}")
                        continue
                
                logger.info(f"Processed {comment_count} comments")
                
            except Exception as e:
                logger.error(f"Error fetching comments: {e}")
            
            # Process any queued notifications
            await self.process_notifications()
            
            self.save_data()
            
        except Exception as e:
            logger.error(f"Error monitoring Reddit: {e}")
            # Try to reinitialize Reddit client on error
            if self.reddit:
                try:
                    await self.reddit.close()
                except:
                    pass
                self.reddit = None
    
    async def add_keyword(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Add a keyword to monitor"""
        if not context.args:
            await update.message.reply_text("Usage: /add <keyword or phrase>\nExample: /add pain killer")
            return
        
        keyword = ' '.join(context.args).lower().strip()
        
        if keyword in self.keywords:
            await update.message.reply_text(f"Already monitoring: {keyword}")
            return
        
        self.keywords.add(keyword)
        self.save_data()
        await update.message.reply_text(f"Added keyword: {keyword}")
        logger.info(f"Added keyword: {keyword}")
    
    async def remove_keyword(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Remove a keyword from monitoring"""
        if not context.args:
            await update.message.reply_text("Usage: /remove <keyword or phrase>")
            return
        
        keyword = ' '.join(context.args).lower().strip()
        
        if keyword not in self.keywords:
            await update.message.reply_text(f"Not monitoring: {keyword}")
            return
        
        self.keywords.remove(keyword)
        self.save_data()
        await update.message.reply_text(f"Removed keyword: {keyword}")
        logger.info(f"Removed keyword: {keyword}")
    
    async def list_keywords(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List all monitored keywords"""
        if not self.keywords:
            await update.message.reply_text("No keywords being monitored.")
            return
        
        keywords_list = '\n'.join(f"- {keyword}" for keyword in sorted(self.keywords))
        message = f"Monitoring {len(self.keywords)} keywords:\n\n{keywords_list}"
        await update.message.reply_text(message)
    
    async def clear_keywords(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Clear all monitored keywords"""
        count = len(self.keywords)
        self.keywords.clear()
        self.save_data()
        await update.message.reply_text(f"Cleared {count} keywords.")
        logger.info(f"Cleared {count} keywords")
    
    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show bot status"""
        status_msg = f"Bot Status:\n\n"
        status_msg += f"Keywords monitored: {len(self.keywords)}\n"
        status_msg += f"Posts processed: {len(self.processed_posts)}\n"
        status_msg += f"Check interval: {self.check_interval} seconds\n"
        status_msg += f"Max posts per check: {self.max_posts}\n"
        status_msg += f"Reddit client: {'Active' if self.reddit else 'Not initialized'}\n"
        status_msg += f"Queued notifications: {len(self.pending_notifications)}"
        
        await update.message.reply_text(status_msg)
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show help message"""
        help_text = """
Reddit to Telegram Monitor Bot

Commands:
/add <keyword> - Add keyword to monitor (e.g., /add pain killer)
/remove <keyword> - Remove keyword from monitoring
/list - List all monitored keywords
/clear - Clear all keywords
/status - Show bot status
/help - Show this help message

The bot monitors all of Reddit for your keywords and sends notifications with the full content and links when matches are found.
        """
        await update.message.reply_text(help_text.strip())
    
    async def monitoring_loop(self):
        """Main monitoring loop"""
        while True:
            try:
                await self.monitor_reddit()
                logger.info(f"Sleeping for {self.check_interval} seconds...")
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                # Close and reset Reddit client on major errors
                if self.reddit:
                    try:
                        await self.reddit.close()
                    except:
                        pass
                    self.reddit = None
                await asyncio.sleep(60)  # Wait 1 minute before retrying
    
    async def start_bot(self):
        """Start the Telegram bot and monitoring"""
        # Initialize Reddit first
        await self.setup_reddit()
        
        # Create application
        app = Application.builder().token(self.telegram_token).build()
        
        # Add handlers
        app.add_handler(CommandHandler("add", self.add_keyword))
        app.add_handler(CommandHandler("remove", self.remove_keyword))
        app.add_handler(CommandHandler("list", self.list_keywords))
        app.add_handler(CommandHandler("clear", self.clear_keywords))
        app.add_handler(CommandHandler("status", self.status))
        app.add_handler(CommandHandler("help", self.help_command))
        app.add_handler(CommandHandler("start", self.help_command))
        
        # Start polling
        await app.initialize()
        await app.start()
        await app.updater.start_polling()
        
        logger.info("Telegram bot started")
        
        # Start monitoring loop
        await self.monitoring_loop()

    async def cleanup(self):
        """Clean up resources"""
        try:
            if self.reddit:
                await self.reddit.close()
        except Exception as e:
            logger.error(f"Error closing Reddit client: {e}")
        
        try:
            if self.session:
                await self.session.close()
        except Exception as e:
            logger.error(f"Error closing HTTP session: {e}")

def main():
    """Main function"""
    # Validate environment variables
    required_vars = [
        'TELEGRAM_BOT_TOKEN',
        'TELEGRAM_CHAT_ID',
        'REDDIT_CLIENT_ID',
        'REDDIT_CLIENT_SECRET'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return
    
    # Create and start bot
    bot = RedditTelegramBot()
    
    try:
        asyncio.run(bot.start_bot())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        asyncio.run(bot.cleanup())
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        asyncio.run(bot.cleanup())

if __name__ == "__main__":
    main()