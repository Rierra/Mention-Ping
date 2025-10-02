#!/usr/bin/env python3
"""
Reddit to Telegram Monitor Bot
Monitors Reddit comprehensively for keywords in BOTH posts and comments
Uses Reddit API + direct subreddit comment streams for complete coverage
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
        self.search_limit = int(os.getenv('SEARCH_LIMIT', '100'))  # results per keyword
        self.search_time_filter = os.getenv('SEARCH_TIME_FILTER', 'hour')  # hour, day, week, month, year, all
        
        # Popular subreddits to monitor for comprehensive coverage
        self.target_subreddits = os.getenv('TARGET_SUBREDDITS', 'all').split(',')
        
        # Data storage
        self.keywords: Set[str] = set()
        self.processed_items: Set[str] = set()  # stores both post and comment IDs
        self.last_search_time: Dict[str, float] = {}
        self.data_file = 'bot_data.json'
        
        # Rate limiting for notifications
        self.notification_delay = 3  # seconds between notifications
        self.pending_notifications = []
        
        # Sessions
        self.telegram_session = None
        self.reddit_session = None
        self.reddit = None
        
        # Stream control
        self.stream_task = None
        self.stop_stream = False
        
        self.load_data()

    def load_data(self):
        """Load keywords and processed items from file"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r') as f:
                    data = json.load(f)
                    self.keywords = set(data.get('keywords', []))
                    self.processed_items = set(data.get('processed_items', []))
                    self.last_search_time = data.get('last_search_time', {})
                    logger.info(f"Loaded {len(self.keywords)} keywords and {len(self.processed_items)} processed items")
            else:
                logger.info("No existing data file found, starting fresh")
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            self.keywords = set()
            self.processed_items = set()
            self.last_search_time = {}

    async def setup_reddit(self):
        """Initialize Reddit API client"""
        try:
            if not self.reddit_session or self.reddit_session.closed:
                timeout = aiohttp.ClientTimeout(total=30, connect=10)
                self.reddit_session = aiohttp.ClientSession(timeout=timeout)
            
            if self.reddit_username and self.reddit_password:
                self.reddit = asyncpraw.Reddit(
                    client_id=self.reddit_client_id,
                    client_secret=self.reddit_client_secret,
                    user_agent=self.reddit_user_agent,
                    username=self.reddit_username,
                    password=self.reddit_password,
                    requestor_kwargs={'session': self.reddit_session}
                )
            else:
                self.reddit = asyncpraw.Reddit(
                    client_id=self.reddit_client_id,
                    client_secret=self.reddit_client_secret,
                    user_agent=self.reddit_user_agent,
                    requestor_kwargs={'session': self.reddit_session}
                )
            
            logger.info("Reddit API initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Reddit API: {e}")
            raise
    
    def save_data(self):
        """Save keywords and processed items to file"""
        try:
            # Limit processed items to prevent file growth
            if len(self.processed_items) > 10000:
                self.processed_items = set(list(self.processed_items)[-5000:])
                
            data = {
                'keywords': list(self.keywords),
                'processed_items': list(self.processed_items),
                'last_search_time': self.last_search_time
            }
            with open(self.data_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving data: {e}")
    
    def contains_phrase(self, text: str, phrase: str) -> bool:
        """Check if text contains the exact phrase (case-insensitive)"""
        if not text or not phrase:
            return False
        pattern = r'\b' + re.escape(phrase.lower()) + r'\b'
        return bool(re.search(pattern, text.lower()))
    
    def format_notification(self, item, keyword: str, item_type: str) -> str:
        """Format notification message"""
        try:
            if item_type == "post":
                title = item.title[:200] + "..." if len(item.title) > 200 else item.title
                content = ""
                
                try:
                    if hasattr(item, 'selftext') and item.selftext:
                        content = item.selftext[:500] + "..." if len(item.selftext) > 500 else item.selftext
                except AttributeError:
                    pass
                
                message = f"🔍 Keyword: {keyword}\n\n"
                message += f"📝 Post: {title}\n"
                message += f"👤 By: u/{item.author}\n"
                message += f"📍 Subreddit: r/{item.subreddit}\n"
                
                if content and content.strip():
                    message += f"\n💬 Content:\n{content}\n"
                
                message += f"\n🔗 https://reddit.com{item.permalink}"
                
            else:  # comment
                content = ""
                
                try:
                    if hasattr(item, 'body') and item.body:
                        content = item.body[:500] + "..." if len(item.body) > 500 else item.body
                except AttributeError:
                    pass
                
                message = f"🔍 Keyword: {keyword}\n\n"
                message += f"💬 Comment by: u/{item.author}\n"
                message += f"📍 Subreddit: r/{item.subreddit}\n"
                
                if content:
                    message += f"\n💭 Comment:\n{content}\n"
                
                message += f"\n🔗 https://reddit.com{item.permalink}"
                
        except AttributeError as e:
            logger.error(f"Error formatting notification: {e}")
            message = f"🔍 Keyword: {keyword}\n\nError formatting item details."
        
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
                
                if self.pending_notifications:
                    await asyncio.sleep(self.notification_delay)
                    
            except Exception as e:
                logger.error(f"Error processing notification: {e}")
                self.pending_notifications.insert(0, message)
                await asyncio.sleep(self.notification_delay * 2)
                break
    
    async def _send_telegram_message(self, message: str):
        """Send a single message to Telegram"""
        try:
            if not self.telegram_session or self.telegram_session.closed:
                timeout = aiohttp.ClientTimeout(total=30)
                self.telegram_session = aiohttp.ClientSession(timeout=timeout)
                
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            data = {
                'chat_id': self.telegram_chat_id,
                'text': message,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            }
            
            async with self.telegram_session.post(url, data=data) as response:
                if response.status == 429:
                    response_json = await response.json()
                    retry_after = response_json.get('parameters', {}).get('retry_after', 60)
                    logger.warning(f"Rate limited, waiting {retry_after} seconds")
                    await asyncio.sleep(retry_after)
                    async with self.telegram_session.post(url, data=data) as retry_response:
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

    async def search_posts(self, keyword: str):
        """Search Reddit posts for keyword"""
        try:
            logger.info(f"Searching posts for keyword: {keyword}")
            
            subreddit = await self.reddit.subreddit('all')
            new_matches = 0
            
            async for post in subreddit.search(
                keyword, 
                sort='new', 
                time_filter=self.search_time_filter, 
                limit=self.search_limit
            ):
                try:
                    if post.id in self.processed_items:
                        continue
                    
                    # Validate exact phrase match
                    title_match = self.contains_phrase(post.title, keyword)
                    body_match = False
                    
                    try:
                        if hasattr(post, 'selftext') and post.selftext:
                            body_match = self.contains_phrase(post.selftext, keyword)
                    except AttributeError:
                        pass

                    if title_match or body_match:
                        new_matches += 1
                        message = self.format_notification(post, keyword, "post")
                        await self.send_notification(message)
                        self.processed_items.add(post.id)
                        logger.info(f"Found matching post: {post.id}")
                    
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Error processing post: {e}")
                    continue
            
            logger.info(f"Post search for '{keyword}': {new_matches} new matches")
            
        except Exception as e:
            logger.error(f"Error searching posts for '{keyword}': {e}")

    async def search_comments_via_posts(self, keyword: str):
        """Search for comments by finding recent posts and checking their comments"""
        try:
            logger.info(f"Searching comments (via posts) for keyword: {keyword}")
            
            subreddit = await self.reddit.subreddit('all')
            new_matches = 0
            
            # Get recent posts to check their comments
            async for post in subreddit.new(limit=self.search_limit):
                try:
                    if post.num_comments == 0:
                        continue
                    
                    # Expand comments
                    await post.comments.replace_more(limit=0)
                    
                    for comment in post.comments.list():
                        try:
                            if comment.id in self.processed_items:
                                continue
                            
                            if hasattr(comment, 'body') and self.contains_phrase(comment.body, keyword):
                                new_matches += 1
                                message = self.format_notification(comment, keyword, "comment")
                                await self.send_notification(message)
                                self.processed_items.add(comment.id)
                                logger.info(f"Found matching comment: {comment.id}")
                            
                        except Exception as e:
                            logger.error(f"Error processing comment: {e}")
                            continue
                    
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Error processing post comments: {e}")
                    continue
            
            logger.info(f"Comment search (via posts) for '{keyword}': {new_matches} new matches")
            
        except Exception as e:
            logger.error(f"Error searching comments via posts for '{keyword}': {e}")

    async def stream_comments(self):
        """Stream new comments from Reddit in real-time"""
        logger.info("Starting comment stream...")
        
        while not self.stop_stream:
            try:
                if not self.reddit:
                    await self.setup_reddit()
                
                subreddit = await self.reddit.subreddit('all')
                
                async for comment in subreddit.stream.comments(skip_existing=True):
                    if self.stop_stream:
                        break
                    
                    try:
                        # Skip if already processed
                        if comment.id in self.processed_items:
                            continue
                        
                        # Check against all keywords
                        for keyword in list(self.keywords):
                            if hasattr(comment, 'body') and self.contains_phrase(comment.body, keyword):
                                message = self.format_notification(comment, keyword, "comment")
                                await self.send_notification(message)
                                self.processed_items.add(comment.id)
                                logger.info(f"Stream found matching comment: {comment.id} for keyword: {keyword}")
                                break  # Only notify once per comment even if multiple keywords match
                        
                    except Exception as e:
                        logger.error(f"Error processing streamed comment: {e}")
                        continue
                
            except Exception as e:
                logger.error(f"Error in comment stream: {e}")
                # Try to recover
                if self.reddit:
                    try:
                        await self.reddit.close()
                    except:
                        pass
                    self.reddit = None
                await asyncio.sleep(30)  # Wait before retrying
    
    async def search_keyword(self, keyword: str):
        """Comprehensive search for a keyword in both posts and comments"""
        try:
            if not self.reddit:
                await self.setup_reddit()
            
            logger.info(f"Starting comprehensive search for: {keyword}")
            
            # Search posts
            await self.search_posts(keyword)
            
            # Search comments via recent posts
            await self.search_comments_via_posts(keyword)
            
            # Update last search time
            self.last_search_time[keyword] = time.time()
            
            logger.info(f"Completed search for: {keyword}")
            
        except Exception as e:
            logger.error(f"Error in comprehensive search for '{keyword}': {e}")
    
    async def monitor_reddit(self):
        """Monitor Reddit for keyword matches using search"""
        if not self.keywords:
            logger.info("No keywords to monitor")
            return
        
        try:
            if not self.reddit:
                await self.setup_reddit()
            
            logger.info(f"Starting search for {len(self.keywords)} keywords...")
            
            for keyword in list(self.keywords):
                try:
                    await self.search_keyword(keyword)
                    await asyncio.sleep(2)  # Delay between keywords
                    
                except Exception as e:
                    logger.error(f"Error processing keyword '{keyword}': {e}")
                    continue
            
            # Process any queued notifications
            await self.process_notifications()
            
            self.save_data()
            
            logger.info("Search cycle completed")
            
        except Exception as e:
            logger.error(f"Error monitoring Reddit: {e}")
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
        await update.message.reply_text(f"✅ Added keyword: {keyword}\n\nNow monitoring in posts, comments, and real-time stream!")
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
        if keyword in self.last_search_time:
            del self.last_search_time[keyword]
        self.save_data()
        await update.message.reply_text(f"✅ Removed keyword: {keyword}")
        logger.info(f"Removed keyword: {keyword}")
    
    async def list_keywords(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List all monitored keywords"""
        if not self.keywords:
            await update.message.reply_text("No keywords being monitored.")
            return
        
        keywords_list = '\n'.join(f"• {keyword}" for keyword in sorted(self.keywords))
        message = f"📋 Monitoring {len(self.keywords)} keywords:\n\n{keywords_list}"
        await update.message.reply_text(message)
    
    async def clear_keywords(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Clear all monitored keywords"""
        count = len(self.keywords)
        self.keywords.clear()
        self.last_search_time.clear()
        self.save_data()
        await update.message.reply_text(f"✅ Cleared {count} keywords.")
        logger.info(f"Cleared {count} keywords")
    
    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show bot status"""
        status_msg = f"📊 Bot Status:\n\n"
        status_msg += f"🔍 Keywords monitored: {len(self.keywords)}\n"
        status_msg += f"✅ Items processed: {len(self.processed_items)}\n"
        status_msg += f"⏱ Check interval: {self.check_interval} seconds\n"
        status_msg += f"📈 Search limit: {self.search_limit} per keyword\n"
        status_msg += f"📅 Time filter: {self.search_time_filter}\n"
        status_msg += f"🤖 Reddit client: {'Active' if self.reddit else 'Not initialized'}\n"
        status_msg += f"📬 Queued notifications: {len(self.pending_notifications)}\n"
        status_msg += f"🌊 Comment stream: {'Running' if self.stream_task and not self.stream_task.done() else 'Stopped'}"
        
        await update.message.reply_text(status_msg)
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show help message"""
        help_text = """
🤖 Reddit to Telegram Monitor Bot

📋 Commands:
/add <keyword> - Add keyword to monitor
/remove <keyword> - Remove keyword
/list - List all monitored keywords
/clear - Clear all keywords
/status - Show bot status
/help - Show this help message

🔍 Features:
• Searches ALL Reddit posts for keywords
• Searches comments in recent posts
• Real-time comment streaming
• Phrase matching (exact words)

📝 Example:
/add pain killer

The bot will find "pain killer" in posts and comments across all of Reddit!
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
                if self.reddit:
                    try:
                        await self.reddit.close()
                    except:
                        pass
                    self.reddit = None
                await asyncio.sleep(60)
    
    async def start_bot(self):
        """Start the Telegram bot and monitoring"""
        # Initialize Reddit
        await self.setup_reddit()
        
        # Create Telegram application
        app = Application.builder().token(self.telegram_token).build()
        
        # Add handlers
        app.add_handler(CommandHandler("add", self.add_keyword))
        app.add_handler(CommandHandler("remove", self.remove_keyword))
        app.add_handler(CommandHandler("list", self.list_keywords))
        app.add_handler(CommandHandler("clear", self.clear_keywords))
        app.add_handler(CommandHandler("status", self.status))
        app.add_handler(CommandHandler("help", self.help_command))
        app.add_handler(CommandHandler("start", self.help_command))
        
        # Start Telegram bot
        await app.initialize()
        await app.start()
        await app.updater.start_polling()
        
        logger.info("Telegram bot started")
        
        # Start comment stream in background
        self.stream_task = asyncio.create_task(self.stream_comments())
        logger.info("Comment stream started")
        
        # Start monitoring loop
        await self.monitoring_loop()

    async def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up resources...")
        
        self.stop_stream = True
        
        if self.stream_task:
            self.stream_task.cancel()
            try:
                await self.stream_task
            except asyncio.CancelledError:
                pass
        
        try:
            if self.reddit:
                await self.reddit.close()
        except Exception as e:
            logger.error(f"Error closing Reddit client: {e}")
        
        try:
            if self.reddit_session and not self.reddit_session.closed:
                await self.reddit_session.close()
        except Exception as e:
            logger.error(f"Error closing Reddit session: {e}")
    
        try:
            if self.telegram_session and not self.telegram_session.closed:
                await self.telegram_session.close()
        except Exception as e:
            logger.error(f"Error closing Telegram session: {e}")

def main():
    """Main function"""
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