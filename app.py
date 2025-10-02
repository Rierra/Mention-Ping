#!/usr/bin/env python3
"""
Reddit to Telegram Monitor Bot - Optimized Version
Focuses on real-time streaming with improved search strategies
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

load_dotenv()

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
        
        self.reddit_username = os.getenv('REDDIT_USERNAME', '')
        self.reddit_password = os.getenv('REDDIT_PASSWORD', '')
        
        # Optimized configuration
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '600'))  # 10 minutes for search
        self.search_limit = int(os.getenv('SEARCH_LIMIT', '100'))
        self.search_time_filter = os.getenv('SEARCH_TIME_FILTER', 'hour')  # Changed to 'hour' for reliability
        
        # Data storage
        self.keywords: Set[str] = set()
        self.processed_items: Set[str] = set()
        self.last_search_time: Dict[str, float] = {}
        self.data_file = 'bot_data.json'
        
        # Rate limiting for notifications
        self.notification_delay = 2
        self.pending_notifications = []
        
        # Sessions
        self.telegram_session = None
        self.reddit_session = None
        self.reddit = None
        
        # Stream control
        self.stream_task = None
        self.stop_stream = False
        
        # Statistics
        self.stats = {
            'stream_comments_checked': 0,
            'stream_matches_found': 0,
            'search_posts_checked': 0,
            'search_matches_found': 0,
            'last_stream_comment': None,
            'stream_start_time': None
        }
        
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
            # Limit processed items to prevent file growth (keep last 24 hours worth)
            if len(self.processed_items) > 50000:
                self.processed_items = set(list(self.processed_items)[-25000:])
                logger.info("Trimmed processed_items to 25000 most recent")
                
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
        """Check if text contains the exact phrase (case-insensitive, word boundaries)"""
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
                        content = item.selftext[:400] + "..." if len(item.selftext) > 400 else item.selftext
                except AttributeError:
                    pass
                
                message = f"[POST] Keyword: {keyword}\n\n"
                message += f"Title: {title}\n"
                message += f"By: u/{item.author}\n"
                message += f"Subreddit: r/{item.subreddit}\n"
                
                if content and content.strip():
                    message += f"\nContent:\n{content}\n"
                
                message += f"\nLink: https://reddit.com{item.permalink}"
                
            else:  # comment
                content = ""
                
                try:
                    if hasattr(item, 'body') and item.body:
                        content = item.body[:400] + "..." if len(item.body) > 400 else item.body
                except AttributeError:
                    pass
                
                message = f"[COMMENT] Keyword: {keyword}\n\n"
                message += f"By: u/{item.author}\n"
                message += f"Subreddit: r/{item.subreddit}\n"
                
                if content:
                    message += f"\nComment:\n{content}\n"
                
                message += f"\nLink: https://reddit.com{item.permalink}"
                
        except AttributeError as e:
            logger.error(f"Error formatting notification: {e}")
            message = f"[{item_type.upper()}] Keyword: {keyword}\n\nError formatting item details."
        
        return message
    
    async def send_notification(self, message: str):
        """Queue notification to be sent with rate limiting"""
        self.pending_notifications.append(message)
    
    async def process_notifications(self):
        """Process queued notifications with rate limiting"""
        while self.pending_notifications:
            try:
                message = self.pending_notifications.pop(0)
                await self._send_telegram_message(message)
                
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

    async def search_with_multiple_sorts(self, keyword: str):
        """Search posts using multiple sort methods to catch more results"""
        try:
            logger.info(f"Multi-sort search for keyword: {keyword}")
            
            subreddit = await self.reddit.subreddit('all')
            new_matches = 0
            sort_methods = ['new', 'hot', 'relevance']
            
            for sort_method in sort_methods:
                try:
                    async for post in subreddit.search(
                        keyword, 
                        sort=sort_method, 
                        time_filter=self.search_time_filter, 
                        limit=self.search_limit
                    ):
                        try:
                            if post.id in self.processed_items:
                                continue
                            
                            self.stats['search_posts_checked'] += 1
                            
                            # Check title and body
                            title_match = self.contains_phrase(post.title, keyword)
                            body_match = False
                            
                            try:
                                if hasattr(post, 'selftext') and post.selftext:
                                    body_match = self.contains_phrase(post.selftext, keyword)
                            except AttributeError:
                                pass

                            if title_match or body_match:
                                new_matches += 1
                                self.stats['search_matches_found'] += 1
                                message = self.format_notification(post, keyword, "post")
                                await self.send_notification(message)
                                self.processed_items.add(post.id)
                                logger.info(f"Found post match (sort={sort_method}): {post.id}")
                            
                            await asyncio.sleep(0.05)
                            
                        except Exception as e:
                            logger.error(f"Error processing post: {e}")
                            continue
                    
                    await asyncio.sleep(1)  # Delay between sort methods
                    
                except Exception as e:
                    logger.error(f"Error with sort method {sort_method}: {e}")
                    continue
            
            logger.info(f"Multi-sort search for '{keyword}': {new_matches} new matches")
            return new_matches
            
        except Exception as e:
            logger.error(f"Error in multi-sort search for '{keyword}': {e}")
            return 0

    async def stream_comments(self):
        """Stream new comments from Reddit in real-time - PRIMARY monitoring method"""
        logger.info("Starting comment stream (primary monitoring method)...")
        self.stats['stream_start_time'] = datetime.now()
        
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while not self.stop_stream:
            try:
                if not self.reddit:
                    await self.setup_reddit()
                
                subreddit = await self.reddit.subreddit('all')
                logger.info("Comment stream connected to r/all")
                consecutive_errors = 0  # Reset on successful connection
                
                async for comment in subreddit.stream.comments(skip_existing=True):
                    if self.stop_stream:
                        break
                    
                    try:
                        self.stats['stream_comments_checked'] += 1
                        self.stats['last_stream_comment'] = datetime.now()
                        
                        # Skip if already processed
                        if comment.id in self.processed_items:
                            continue
                        
                        # Check against all keywords
                        for keyword in list(self.keywords):
                            if hasattr(comment, 'body') and self.contains_phrase(comment.body, keyword):
                                self.stats['stream_matches_found'] += 1
                                message = self.format_notification(comment, keyword, "comment")
                                await self.send_notification(message)
                                self.processed_items.add(comment.id)
                                logger.info(f"Stream match: comment {comment.id} for keyword '{keyword}'")
                                break
                        
                        # Process notifications periodically
                        if len(self.pending_notifications) >= 10:
                            await self.process_notifications()
                        
                        # Periodic save every 1000 comments
                        if self.stats['stream_comments_checked'] % 1000 == 0:
                            self.save_data()
                            logger.info(f"Stream stats: {self.stats['stream_comments_checked']} checked, {self.stats['stream_matches_found']} matches")
                        
                    except Exception as e:
                        logger.error(f"Error processing streamed comment: {e}")
                        continue
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Error in comment stream (attempt {consecutive_errors}/{max_consecutive_errors}): {e}")
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error("Too many consecutive stream errors, waiting longer before retry")
                    await asyncio.sleep(300)  # 5 minute cooldown
                    consecutive_errors = 0
                else:
                    await asyncio.sleep(30)
                
                # Clean up and reconnect
                if self.reddit:
                    try:
                        await self.reddit.close()
                    except:
                        pass
                    self.reddit = None
    
    async def search_keyword(self, keyword: str):
        """Search using multiple methods - SUPPLEMENTARY to stream"""
        try:
            if not self.reddit:
                await self.setup_reddit()
            
            logger.info(f"Starting supplementary search for: {keyword}")
            
            # Use multi-sort search for better coverage
            await self.search_with_multiple_sorts(keyword)
            
            # Update last search time
            self.last_search_time[keyword] = time.time()
            
            logger.info(f"Completed supplementary search for: {keyword}")
            
        except Exception as e:
            logger.error(f"Error in supplementary search for '{keyword}': {e}")
    
    async def monitor_reddit(self):
        """Supplementary search monitoring (stream is primary)"""
        if not self.keywords:
            logger.info("No keywords to search for")
            return
        
        try:
            if not self.reddit:
                await self.setup_reddit()
            
            logger.info(f"Starting supplementary search for {len(self.keywords)} keywords...")
            
            for keyword in list(self.keywords):
                try:
                    await self.search_keyword(keyword)
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing keyword '{keyword}': {e}")
                    continue
            
            # Process any queued notifications
            await self.process_notifications()
            
            self.save_data()
            
            logger.info("Search cycle completed")
            
        except Exception as e:
            logger.error(f"Error in supplementary monitoring: {e}")
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
        await update.message.reply_text(
            f"Added keyword: {keyword}\n\n"
            f"Stream will catch this in real-time.\n"
            f"Search will backfill recent content."
        )
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
        self.last_search_time.clear()
        self.save_data()
        await update.message.reply_text(f"Cleared {count} keywords.")
        logger.info(f"Cleared {count} keywords")
    
    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show bot status with statistics"""
        stream_running = self.stream_task and not self.stream_task.done()
        
        status_msg = f"Bot Status:\n\n"
        status_msg += f"Keywords: {len(self.keywords)}\n"
        status_msg += f"Processed items: {len(self.processed_items)}\n"
        status_msg += f"Pending notifications: {len(self.pending_notifications)}\n\n"
        
        status_msg += f"Configuration:\n"
        status_msg += f"- Search interval: {self.check_interval}s\n"
        status_msg += f"- Search time filter: {self.search_time_filter}\n"
        status_msg += f"- Search limit: {self.search_limit}\n\n"
        
        status_msg += f"Stream Statistics:\n"
        status_msg += f"- Status: {'Running' if stream_running else 'Stopped'}\n"
        status_msg += f"- Comments checked: {self.stats['stream_comments_checked']}\n"
        status_msg += f"- Matches found: {self.stats['stream_matches_found']}\n"
        
        if self.stats['last_stream_comment']:
            time_since = (datetime.now() - self.stats['last_stream_comment']).seconds
            status_msg += f"- Last comment: {time_since}s ago\n"
        
        if self.stats['stream_start_time']:
            uptime = datetime.now() - self.stats['stream_start_time']
            status_msg += f"- Uptime: {str(uptime).split('.')[0]}\n"
        
        status_msg += f"\nSearch Statistics:\n"
        status_msg += f"- Posts checked: {self.stats['search_posts_checked']}\n"
        status_msg += f"- Matches found: {self.stats['search_matches_found']}"
        
        await update.message.reply_text(status_msg)
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show help message"""
        help_text = """
Reddit to Telegram Monitor Bot

Commands:
/add <keyword> - Add keyword to monitor
/remove <keyword> - Remove keyword
/list - List all monitored keywords
/clear - Clear all keywords
/status - Show bot status with statistics
/help - Show this help message

How it works:
- Real-time stream monitors ALL new comments (primary)
- Periodic search backfills recent posts (supplementary)
- Stream catches everything going forward
- Search helps catch posts and backfill gaps

Note: Reddit's API search is limited compared to web search.
The stream is your main monitoring tool.

Example:
/add pain killer
        """
        await update.message.reply_text(help_text.strip())
    
    async def monitoring_loop(self):
        """Supplementary search loop (stream is primary)"""
        # Wait a bit before starting searches to let stream establish
        await asyncio.sleep(30)
        
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
        
        # Start comment stream (PRIMARY monitoring)
        self.stream_task = asyncio.create_task(self.stream_comments())
        logger.info("Comment stream started (primary monitoring)")
        
        # Start search loop (SUPPLEMENTARY monitoring)
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
