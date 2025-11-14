#!/usr/bin/env python3
"""
Reddit to Telegram Monitor Bot - Multi-Group Version (Fixed)
Monitors Reddit comprehensively for keywords in BOTH posts and comments
Owner controls all groups from main control group
"""

import os
import json
import time
import logging
import asyncio
import re
import aiohttp
import html
from typing import Set, List, Dict, Optional
from datetime import datetime, timedelta

import asyncpraw
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler
from dotenv import load_dotenv
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError

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
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')  # Owner's control group
        self.owner_chat_id = int(self.telegram_chat_id)  # Store as int for comparison
        self.slack_bot_token = os.getenv('SLACK_BOT_TOKEN')  # Optional Slack token
        
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
        
        # Multi-group data storage
        # group_id -> {name: str, keywords: set, enabled: bool, platform: str, channel_id: str, workspace_id: str (for slack)}
        self.groups: Dict[int, Dict] = {}
        self.processed_items: Dict[int, Set[str]] = {}  # group_id -> set of processed item IDs
        self.last_search_time: Dict[str, float] = {}  # "group_id:keyword" -> timestamp
        
        # Slack workspaces storage: workspace_id -> {name: str, token: str}
        self.slack_workspaces: Dict[str, Dict] = {}
        
        # Temporary state for adding keywords and menu navigation
        self.pending_keyword_add: Dict[int, int] = {}  # user_id -> selected_group_id
        self.pending_keyword_remove: Dict[int, int] = {}  # user_id -> selected_group_id
        # Temporary state for managing subreddits
        self.pending_subreddit_add: Dict[int, int] = {}  # user_id -> selected_group_id
        self.pending_subreddit_remove: Dict[int, int] = {}  # user_id -> selected_group_id
        # Temporary state for managing subreddit blacklist
        self.pending_subreddit_blacklist_add: Dict[int, int] = {}
        self.pending_subreddit_blacklist_remove: Dict[int, int] = {}
        # Temporary state for managing case-sensitive keywords
        self.pending_case_keyword_add: Dict[int, int] = {}  # user_id -> selected_group_id
        self.pending_case_keyword_remove: Dict[int, int] = {}  # user_id -> selected_group_id
        self.menu_state: Dict[int, str] = {}  # user_id -> current_menu_state
        
        # Use persistent data directory (works on Render with persistent disk, or local ./data directory)
        self.data_dir = os.getenv('DATA_DIR', './data')
        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.data_file = os.path.join(self.data_dir, 'bot_data.json')
        self.data_env_var = 'BOT_DATA_JSON'  # Environment variable for persistent storage (fallback)
        
        # Rate limiting for notifications
        self.notification_delay = 3  # seconds between notifications
        self.pending_notifications = []
        self.notification_lock = asyncio.Lock()
        
        # Sessions
        self.telegram_session: Optional[aiohttp.ClientSession] = None
        self.reddit_session: Optional[aiohttp.ClientSession] = None
        self.reddit: Optional[asyncpraw.Reddit] = None
        self.slack_clients: Dict[str, AsyncWebClient] = {}  # workspace_id -> client
        
        # Background tasks
        self.stream_task: Optional[asyncio.Task] = None
        self.notification_task: Optional[asyncio.Task] = None
        self.stop_stream = False
        self.stop_notification_processor = False
        
        # Rate limiting for Reddit API
        self.last_reddit_request = 0
        self.reddit_request_delay = 2  # seconds between requests
        
        self.load_data()

    def load_data(self):
        """Load groups, keywords and processed items from environment variable or file"""
        try:
            data = None
            
            # Try loading from environment variable first (persists across Render deploys)
            env_data = os.getenv(self.data_env_var)
            if env_data:
                try:
                    data = json.loads(env_data)
                    logger.info("Loaded data from environment variable")
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse environment variable data: {e}, falling back to file")
            
            # Fall back to file if environment variable not available
            if data is None and os.path.exists(self.data_file):
                with open(self.data_file, 'r') as f:
                    data = json.load(f)
                    logger.info("Loaded data from file")
            
            if data:
                # Load groups with keywords as sets
                groups_data = data.get('groups', {})
                self.groups = {}
                for group_id_str, group_info in groups_data.items():
                    group_id = int(group_id_str)
                    # Backward compatibility: default to telegram if platform not specified
                    platform = group_info.get('platform', 'telegram')
                    self.groups[group_id] = {
                        'name': group_info.get('name', f'Group {group_id}'),
                        'keywords': set(group_info.get('keywords', [])),
                        'case_sensitive_keywords': set(group_info.get('case_sensitive_keywords', [])),  # Case-sensitive keywords
                        # Empty or missing subreddits means ALL subreddits
                        'subreddits': set(group_info.get('subreddits', [])),
                        'subreddit_blacklist': set(
                            s.lower() for s in group_info.get('subreddit_blacklist', [])
                        ),
                        'enabled': group_info.get('enabled', True),
                        'platform': platform,
                        'channel_id': group_info.get('channel_id', str(group_id)),
                        'workspace_id': group_info.get('workspace_id', '')  # For slack groups
                    }
                
                # Load Slack workspaces
                self.slack_workspaces = data.get('slack_workspaces', {})
                
                # Load processed items per group
                processed_data = data.get('processed_items', {})
                self.processed_items = {}
                for group_id_str, items in processed_data.items():
                    self.processed_items[int(group_id_str)] = set(items)
                
                self.last_search_time = data.get('last_search_time', {})
                
                # Ensure owner's group exists (always Telegram)
                if self.owner_chat_id not in self.groups:
                    self.groups[self.owner_chat_id] = {
                        'name': 'Control Group (Owner)',
                        'keywords': set(),
                        'case_sensitive_keywords': set(),
                        'subreddits': set(),
                        'subreddit_blacklist': set(),
                        'enabled': True,
                        'platform': 'telegram',
                        'channel_id': str(self.owner_chat_id)
                    }
                
                total_keywords = sum(len(g['keywords']) for g in self.groups.values())
                logger.info(f"Loaded {len(self.groups)} groups with {total_keywords} total keywords")
            else:
                # Initialize with owner's group (always Telegram)
                self.groups = {
                    self.owner_chat_id: {
                        'name': 'Control Group (Owner)',
                        'keywords': set(),
                        'case_sensitive_keywords': set(),
                        'subreddits': set(),
                        'subreddit_blacklist': set(),
                        'enabled': True,
                        'platform': 'telegram',
                        'channel_id': str(self.owner_chat_id)
                    }
                }
                self.processed_items = {}
                self.last_search_time = {}
                logger.info("No existing data found, starting fresh with owner's group")
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            self.groups = {
                self.owner_chat_id: {
                    'name': 'Control Group (Owner)',
                    'keywords': set(),
                    'case_sensitive_keywords': set(),
                    'subreddits': set(),
                    'subreddit_blacklist': set(),
                    'enabled': True,
                    'platform': 'telegram',
                    'channel_id': str(self.owner_chat_id)
                }
            }
            self.processed_items = {}
            self.last_search_time = {}

    async def setup_reddit(self):
        """Initialize Reddit API client"""
        try:
            # Close existing session if any
            if self.reddit_session and not self.reddit_session.closed:
                await self.reddit_session.close()
            
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
    
    async def setup_slack(self):
        """Initialize Slack API clients for all workspaces"""
        try:
            # Initialize clients for all stored workspaces
            for workspace_id, workspace_info in self.slack_workspaces.items():
                token = workspace_info.get('token')
                if token:
                    self.slack_clients[workspace_id] = AsyncWebClient(token=token)
                    logger.info(f"Slack client initialized for workspace: {workspace_info.get('name', workspace_id)}")
            
            if not self.slack_clients:
                logger.info("No Slack workspaces configured. Use /addslack to add one.")
        except Exception as e:
            logger.error(f"Failed to initialize Slack API: {e}")
            # Don't raise - Slack is optional
    
    def save_data(self):
        """Save groups, keywords and processed items to environment variable and file"""
        try:
            # Trim processed items during save
            for group_id in list(self.processed_items.keys()):
                if len(self.processed_items[group_id]) > 10000:
                    self.processed_items[group_id] = set(list(self.processed_items[group_id])[-5000:])
            
            # Convert groups data to JSON-serializable format
            groups_data = {}
            for group_id, group_info in self.groups.items():
                groups_data[str(group_id)] = {
                    'name': group_info['name'],
                    'keywords': list(group_info['keywords']),
                    'case_sensitive_keywords': list(group_info.get('case_sensitive_keywords', set())),
                    'subreddits': list(group_info.get('subreddits', set())),
                    'subreddit_blacklist': list(group_info.get('subreddit_blacklist', set())),
                    'enabled': group_info['enabled'],
                    'platform': group_info.get('platform', 'telegram'),
                    'channel_id': group_info.get('channel_id', str(group_id)),
                    'workspace_id': group_info.get('workspace_id', '')
                }
            
            # Convert processed items to JSON-serializable format
            processed_data = {}
            for group_id, items in self.processed_items.items():
                processed_data[str(group_id)] = list(items)
            
            data = {
                'groups': groups_data,
                'processed_items': processed_data,
                'last_search_time': self.last_search_time,
                'slack_workspaces': self.slack_workspaces
            }
            
            # Save to file (for local development and backup)
            try:
                with open(self.data_file, 'w') as f:
                    json.dump(data, f, indent=2)
                logger.debug("Saved data to file")
            except Exception as e:
                logger.warning(f"Failed to save data to file: {e}")
            
            # Save compact JSON export (for manual environment variable updates on Render)
            try:
                data_json = json.dumps(data, separators=(',', ':'))  # Compact JSON for env var
                env_file = os.path.join(self.data_dir, 'bot_data_env_export.txt')
                with open(env_file, 'w') as f:
                    f.write(data_json)
                logger.debug(f"Saved data export to {env_file} - copy this to BOT_DATA_JSON env var on Render if needed")
            except Exception as e:
                logger.warning(f"Failed to save data export file: {e}")
                
        except Exception as e:
            logger.error(f"Error saving data: {e}")
    
    def trim_processed_items_in_memory(self):
        """Trim processed items during runtime to prevent memory growth"""
        for group_id in list(self.processed_items.keys()):
            if len(self.processed_items[group_id]) > 15000:
                logger.info(f"Trimming processed items for group {group_id}")
                self.processed_items[group_id] = set(list(self.processed_items[group_id])[-7500:])
    
    def is_owner(self, chat_id: int) -> bool:
        """Check if the chat is the owner's control group"""
        return chat_id == self.owner_chat_id
    
    def contains_phrase(self, text: str, phrase: str) -> bool:
        """Check if text contains the exact phrase (case-insensitive)"""
        if not text or not phrase:
            return False
        pattern = r'\b' + re.escape(phrase.lower()) + r'\b'
        return bool(re.search(pattern, text.lower()))
    
    def contains_phrase_case_sensitive(self, text: str, phrase: str) -> bool:
        """Check if text contains the exact phrase (case-sensitive)"""
        if not text or not phrase:
            return False
        pattern = r'\b' + re.escape(phrase) + r'\b'
        return bool(re.search(pattern, text))
    
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
                
                message = f"Keyword: {keyword}\n\n"
                message += f"Post: {title}\n"
                message += f"By: u/{item.author}\n"
                message += f"Subreddit: r/{item.subreddit}\n"
                
                if content and content.strip():
                    message += f"\nContent:\n{content}\n"
                
                message += f"\nLink: https://reddit.com{item.permalink}"
                
            else:  # comment
                content = ""
                
                try:
                    if hasattr(item, 'body') and item.body:
                        content = item.body[:500] + "..." if len(item.body) > 500 else item.body
                except AttributeError:
                    pass
                
                message = f"Keyword: {keyword}\n\n"
                message += f"Comment by: u/{item.author}\n"
                message += f"Subreddit: r/{item.subreddit}\n"
                
                if content:
                    message += f"\nComment:\n{content}\n"
                
                message += f"\nLink: https://reddit.com{item.permalink}"
                
        except AttributeError as e:
            logger.error(f"Error formatting notification: {e}")
            message = f"Keyword: {keyword}\n\nError formatting item details."
        
        return message
    
    async def send_notification_to_group(self, group_id: int, message: str):
        """Queue notification to be sent to specific group with rate limiting"""
        async with self.notification_lock:
            self.pending_notifications.append((group_id, message))
            logger.info(f"Queued notification for group {group_id}, {len(self.pending_notifications)} in queue")
    
    async def notification_processor(self):
        """Continuously process queued notifications with rate limiting"""
        logger.info("Notification processor started")
        
        while not self.stop_notification_processor:
            try:
                async with self.notification_lock:
                    if self.pending_notifications:
                        group_id, message = self.pending_notifications.pop(0)
                    else:
                        group_id, message = None, None
                
                if group_id is not None:
                    try:
                        await self._send_platform_message(group_id, message)
                        logger.info(f"Notification sent to group {group_id} successfully")
                        await asyncio.sleep(self.notification_delay)
                    except Exception as e:
                        logger.error(f"Error sending notification: {e}")
                        # Re-queue the failed notification
                        async with self.notification_lock:
                            self.pending_notifications.insert(0, (group_id, message))
                        await asyncio.sleep(self.notification_delay * 2)
                else:
                    # No notifications, sleep briefly
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logger.error(f"Error in notification processor: {e}")
                await asyncio.sleep(5)
        
        logger.info("Notification processor stopped")
    
    async def _send_telegram_message(self, chat_id: int, message: str):
        """Send a single message to Telegram"""
        try:
            if not self.telegram_session or self.telegram_session.closed:
                timeout = aiohttp.ClientTimeout(total=30)
                self.telegram_session = aiohttp.ClientSession(timeout=timeout)
                
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            data = {
                'chat_id': chat_id,
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
            logger.error(f"Error sending Telegram notification: {e}")
            raise
    
    async def _send_slack_message(self, workspace_id: str, channel_id: str, message: str):
        """Send a single message to Slack"""
        try:
            if workspace_id not in self.slack_clients:
                raise Exception(f"Slack client not initialized for workspace: {workspace_id}")
            
            slack_client = self.slack_clients[workspace_id]
            
            # Convert message formatting if needed (Slack uses mrkdwn)
            # Keep it simple for now - Slack handles plain text well
            
            response = await slack_client.chat_postMessage(
                channel=channel_id,
                text=message,
                unfurl_links=False
            )
            
            if not response['ok']:
                raise Exception(f"Slack API error: {response.get('error', 'Unknown error')}")
                
        except SlackApiError as e:
            logger.error(f"Slack API error: {e.response['error']}")
            raise
        except Exception as e:
            logger.error(f"Error sending Slack notification: {e}")
            raise
    
    async def _send_platform_message(self, group_id: int, message: str):
        """Send message to the appropriate platform based on group configuration"""
        if group_id not in self.groups:
            logger.error(f"Group {group_id} not found")
            return
        
        group_info = self.groups[group_id]
        platform = group_info.get('platform', 'telegram')
        channel_id = group_info.get('channel_id', str(group_id))
        
        if platform == 'slack':
            workspace_id = group_info.get('workspace_id', '')
            await self._send_slack_message(workspace_id, channel_id, message)
        else:  # Default to telegram
            await self._send_telegram_message(int(channel_id), message)

    async def rate_limit_reddit_request(self):
        """Ensure proper spacing between Reddit API requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_reddit_request
        
        if time_since_last < self.reddit_request_delay:
            await asyncio.sleep(self.reddit_request_delay - time_since_last)
        
        self.last_reddit_request = time.time()

    async def search_posts(self, group_id: int, keyword: str, case_sensitive: bool = False):
        """Search Reddit posts for keyword for a specific group"""
        try:
            logger.info(f"Searching posts for keyword: {keyword} (Group: {group_id}, Case-sensitive: {case_sensitive})")
            
            # Initialize processed items set for this group if not exists
            if group_id not in self.processed_items:
                self.processed_items[group_id] = set()
            
            await self.rate_limit_reddit_request()

            # Determine which subreddits to search: specific list or 'all'
            group_info = self.groups.get(group_id, {})
            subreddits: Set[str] = set(group_info.get('subreddits', set()))
            blacklist: Set[str] = set(group_info.get('subreddit_blacklist', set()))
            targets = list(subreddits) if subreddits else ['all']

            new_matches = 0

            # Choose matching function based on case sensitivity
            match_func = self.contains_phrase_case_sensitive if case_sensitive else self.contains_phrase

            for sr in targets:
                if sr in blacklist:
                    logger.debug(f"Skipping subreddit {sr} for group {group_id} (blacklisted)")
                    continue
                subreddit = await self.reddit.subreddit(sr)

                async for post in subreddit.search(
                    keyword,
                    sort='new',
                    time_filter=self.search_time_filter,
                    limit=self.search_limit
                ):
                    try:
                        if post.id in self.processed_items[group_id]:
                            continue

                        # Validate exact phrase match (case-sensitive or case-insensitive)
                        title_match = match_func(post.title, keyword)
                        body_match = False

                        try:
                            if hasattr(post, 'selftext') and post.selftext:
                                body_match = match_func(post.selftext, keyword)
                        except AttributeError:
                            pass

                        # If subreddits are specified, ensure post's subreddit matches
                        if subreddits:
                            try:
                                post_sr = str(post.subreddit).lower()
                            except Exception:
                                post_sr = ""
                            if post_sr not in subreddits:
                                continue
                        else:
                            try:
                                post_sr = str(post.subreddit).lower()
                            except Exception:
                                post_sr = ""

                        # Skip if subreddit is blacklisted
                        if blacklist:
                            if not post_sr:
                                try:
                                    post_sr = str(post.subreddit).lower()
                                except Exception:
                                    post_sr = ""
                            if post_sr in blacklist:
                                continue

                        if title_match or body_match:
                            new_matches += 1
                            message = self.format_notification(post, keyword, "post")
                            await self.send_notification_to_group(group_id, message)
                            self.processed_items[group_id].add(post.id)
                            logger.info(f"Found matching post: {post.id} for group {group_id}")

                        await asyncio.sleep(0.1)

                    except Exception as e:
                        logger.error(f"Error processing post: {e}")
                        continue
            
            logger.info(f"Post search for '{keyword}' (Group {group_id}): {new_matches} new matches")
            
        except Exception as e:
            logger.error(f"Error searching posts for '{keyword}' (Group {group_id}): {e}")

    async def search_comments_via_posts(self, group_id: int, keyword: str, case_sensitive: bool = False):
        """Search for comments by finding recent posts and checking their comments"""
        try:
            logger.info(f"Searching comments (via posts) for keyword: {keyword} (Group: {group_id}, Case-sensitive: {case_sensitive})")
            
            if group_id not in self.processed_items:
                self.processed_items[group_id] = set()
            
            await self.rate_limit_reddit_request()

            group_info = self.groups.get(group_id, {})
            subreddits: Set[str] = set(group_info.get('subreddits', set()))
            blacklist: Set[str] = set(group_info.get('subreddit_blacklist', set()))
            targets = list(subreddits) if subreddits else ['all']

            new_matches = 0
            
            # Choose matching function based on case sensitivity
            match_func = self.contains_phrase_case_sensitive if case_sensitive else self.contains_phrase

            # Iterate target subreddits
            for sr in targets:
                if sr in blacklist:
                    logger.debug(f"Skipping subreddit {sr} for group {group_id} (blacklisted)")
                    continue
                subreddit = await self.reddit.subreddit(sr)
                # Get recent posts to check their comments
                async for post in subreddit.new(limit=self.search_limit):
                    try:
                        if post.num_comments == 0:
                            continue
                        # Check if comments exist and are accessible
                        if not hasattr(post, 'comments') or post.comments is None:
                            logger.debug(f"Post {post.id} has no accessible comments")
                            continue
                        # Expand comments
                        try:
                            await post.comments.replace_more(limit=0)
                        except Exception as e:
                            logger.debug(f"Could not expand comments for post {post.id}: {e}")
                            continue
                        # Get the comments list safely
                        try:
                            comments_list = post.comments.list()
                        except Exception as e:
                            logger.debug(f"Could not get comments list for post {post.id}: {e}")
                            continue
                        # Check if comments_list is valid and iterable
                        if comments_list is None:
                            logger.debug(f"Comments list is None for post {post.id}")
                            continue
                        for comment in comments_list:
                            try:
                                if comment.id in self.processed_items[group_id]:
                                    continue
                                if hasattr(comment, 'body') and match_func(comment.body, keyword):
                                    # Determine comment subreddit
                                    try:
                                        c_sr = str(comment.subreddit).lower()
                                    except Exception:
                                        c_sr = ""
                                    # If subreddits whitelist specified, ensure match
                                    if subreddits and c_sr not in subreddits:
                                        continue
                                    # Apply blacklist (always)
                                    if blacklist and c_sr in blacklist:
                                        continue
                                    new_matches += 1
                                    message = self.format_notification(comment, keyword, "comment")
                                    await self.send_notification_to_group(group_id, message)
                                    self.processed_items[group_id].add(comment.id)
                                    logger.info(f"Found matching comment: {comment.id} for group {group_id}")
                            except Exception as e:
                                logger.error(f"Error processing comment: {e}")
                                continue
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logger.error(f"Error processing post comments: {e}")
                        continue
            
            logger.info(f"Comment search (via posts) for '{keyword}' (Group {group_id}): {new_matches} new matches")
            
        except Exception as e:
            logger.error(f"Error searching comments via posts for '{keyword}' (Group {group_id}): {e}")
        
    async def stream_comments(self):
        """Stream new comments from Reddit in real-time for all groups"""
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
                        # Check against all groups and their keywords
                        for group_id, group_info in self.groups.items():
                            if not group_info['enabled']:
                                continue
                            
                            if group_id not in self.processed_items:
                                self.processed_items[group_id] = set()
                            
                            # Skip if already processed for this group
                            if comment.id in self.processed_items[group_id]:
                                continue
                            
                            # If group limits subreddits, filter first
                            subreddits: Set[str] = set(group_info.get('subreddits', set()))
                            blacklist: Set[str] = set(group_info.get('subreddit_blacklist', set()))
                            try:
                                c_sr = str(comment.subreddit).lower()
                            except Exception:
                                c_sr = ""
                            if subreddits and c_sr not in subreddits:
                                continue
                            if blacklist and c_sr in blacklist:
                                continue

                            # Check against all regular (case-insensitive) keywords for this group
                            matched = False
                            matched_keyword = None
                            
                            for keyword in list(group_info['keywords']):
                                if hasattr(comment, 'body') and self.contains_phrase(comment.body, keyword):
                                    matched = True
                                    matched_keyword = keyword
                                    break  # Found a match, stop checking more regular keywords
                            
                            # If no regular keyword match, check case-sensitive keywords
                            if not matched:
                                case_keywords = group_info.get('case_sensitive_keywords', set())
                                for keyword in list(case_keywords):
                                    if hasattr(comment, 'body') and self.contains_phrase_case_sensitive(comment.body, keyword):
                                        matched = True
                                        matched_keyword = keyword
                                        break  # Found a match, stop checking more case-sensitive keywords
                            
                            # Notify once per comment per group if any keyword matched
                            if matched and matched_keyword:
                                message = self.format_notification(comment, matched_keyword, "comment")
                                await self.send_notification_to_group(group_id, message)
                                self.processed_items[group_id].add(comment.id)
                                logger.info(f"Stream found matching comment: {comment.id} for group {group_id}, keyword: {matched_keyword}")
                        
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
    
    async def search_keyword_for_group(self, group_id: int, keyword: str, case_sensitive: bool = False):
        """Comprehensive search for a keyword in both posts and comments for a specific group"""
        try:
            if not self.reddit:
                await self.setup_reddit()
            
            logger.info(f"Starting comprehensive search for: {keyword} (Group: {group_id}, Case-sensitive: {case_sensitive})")
            
            # Search posts
            await self.search_posts(group_id, keyword, case_sensitive)
            
            # Search comments via recent posts
            await self.search_comments_via_posts(group_id, keyword, case_sensitive)
            
            # Update last search time
            search_key = f"{group_id}:{keyword}"
            self.last_search_time[search_key] = time.time()
            
            logger.info(f"Completed search for: {keyword} (Group: {group_id})")
            
        except Exception as e:
            logger.error(f"Error in comprehensive search for '{keyword}' (Group {group_id}): {e}")
            # Don't re-raise, continue with other keywords
    
    async def monitor_reddit(self):
        """Monitor Reddit for keyword matches using search for all groups"""
        try:
            if not self.reddit:
                await self.setup_reddit()
            
            total_keywords = sum(len(g['keywords']) for g in self.groups.values() if g['enabled'])
            total_case_keywords = sum(len(g.get('case_sensitive_keywords', set())) for g in self.groups.values() if g['enabled'])
            
            if total_keywords == 0 and total_case_keywords == 0:
                logger.info("No keywords to monitor across all groups")
                return
            
            logger.info(f"Starting search for {total_keywords} regular + {total_case_keywords} case-sensitive keywords across {len(self.groups)} groups...")
            
            for group_id, group_info in self.groups.items():
                if not group_info['enabled']:
                    continue
                
                # Search regular (case-insensitive) keywords
                for keyword in list(group_info['keywords']):
                    try:
                        await self.search_keyword_for_group(group_id, keyword, case_sensitive=False)
                        
                    except Exception as e:
                        logger.error(f"Error processing keyword '{keyword}' for group {group_id}: {e}")
                        continue
                
                # Search case-sensitive keywords
                case_keywords = group_info.get('case_sensitive_keywords', set())
                for keyword in list(case_keywords):
                    try:
                        await self.search_keyword_for_group(group_id, keyword, case_sensitive=True)
                        
                    except Exception as e:
                        logger.error(f"Error processing case-sensitive keyword '{keyword}' for group {group_id}: {e}")
                        continue
            
            # Trim processed items in memory if needed
            self.trim_processed_items_in_memory()
            
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
    
    # ============= COMMAND HANDLERS =============
    
    async def addslack(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Add a Slack workspace with its token (Owner only)"""
        chat_id = update.effective_chat.id
        
        if not self.is_owner(chat_id):
            await update.message.reply_text("You don't have permission to use this command. Please contact the bot owner.")
            return
        
        if not context.args or len(context.args) < 2:
            await update.message.reply_text(
                "Usage: /addslack <workspace_id> <bot_token> [workspace_name]\n\n"
                "Example: /addslack T01234ABCDE xoxb-1234-5678-abcd My Workspace\n\n"
                "Note: workspace_id is a unique identifier (can be anything like 'work' or 'team1')"
            )
            return
        
        try:
            workspace_id = context.args[0].lower()
            token = context.args[1]
            workspace_name = ' '.join(context.args[2:]) if len(context.args) > 2 else workspace_id
            
            # Validate token format
            if not token.startswith('xoxb-'):
                await update.message.reply_text("Invalid token format. Slack bot tokens should start with 'xoxb-'")
                return
            
            # Check if workspace already exists
            if workspace_id in self.slack_workspaces:
                await update.message.reply_text(
                    f"Workspace '{workspace_id}' already exists. Use /removeslack first to replace it."
                )
                return
            
            # Test the token by creating a client and making a test call
            try:
                test_client = AsyncWebClient(token=token)
                response = await test_client.auth_test()
                if not response['ok']:
                    await update.message.reply_text(f"Invalid Slack token. Error: {response.get('error', 'Unknown')}")
                    return
                
                # Store workspace info
                self.slack_workspaces[workspace_id] = {
                    'name': workspace_name,
                    'token': token,
                    'team_name': response.get('team', 'Unknown'),
                    'bot_user_id': response.get('user_id', '')
                }
                
                # Initialize client
                self.slack_clients[workspace_id] = test_client
                
                self.save_data()
                
                await update.message.reply_text(
                    f"✅ Slack workspace added successfully!\n\n"
                    f"Workspace ID: {workspace_id}\n"
                    f"Name: {workspace_name}\n"
                    f"Team: {response.get('team', 'Unknown')}\n\n"
                    f"You can now add Slack channels using:\n"
                    f"/addgroup slack:{workspace_id}:C01234ABCDE Channel Name"
                )
                logger.info(f"Added Slack workspace: {workspace_id} ({workspace_name})")
                
            except SlackApiError as e:
                await update.message.reply_text(f"Failed to authenticate with Slack: {e.response['error']}")
            except Exception as e:
                await update.message.reply_text(f"Error testing Slack token: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error adding Slack workspace: {e}")
            await update.message.reply_text(f"Error adding Slack workspace: {e}")
    
    async def listslack(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List all configured Slack workspaces (Owner only)"""
        chat_id = update.effective_chat.id
        
        if not self.is_owner(chat_id):
            await update.message.reply_text("You don't have permission to use this command. Please contact the bot owner.")
            return
        
        if not self.slack_workspaces:
            await update.message.reply_text(
                "No Slack workspaces configured.\n\n"
                "Use /addslack to add one."
            )
            return
        
        message = "Configured Slack Workspaces:\n\n"
        for workspace_id, workspace_info in self.slack_workspaces.items():
            status = "✓" if workspace_id in self.slack_clients else "✗"
            message += f"{status} {workspace_info.get('name', workspace_id)}\n"
            message += f"   ID: {workspace_id}\n"
            message += f"   Team: {workspace_info.get('team_name', 'Unknown')}\n"
            message += f"   Bot User: {workspace_info.get('bot_user_id', 'Unknown')}\n\n"
        
        message += "\nUse /addgroup slack:<workspace_id>:<channel_id> to add channels"
        await update.message.reply_text(message)
    
    async def removeslack(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Remove a Slack workspace (Owner only)"""
        chat_id = update.effective_chat.id
        
        if not self.is_owner(chat_id):
            await update.message.reply_text("You don't have permission to use this command. Please contact the bot owner.")
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /removeslack <workspace_id>")
            return
        
        try:
            workspace_id = context.args[0].lower()
            
            if workspace_id not in self.slack_workspaces:
                await update.message.reply_text(f"Workspace '{workspace_id}' not found.")
                return
            
            # Check if any groups are using this workspace
            groups_using = [g['name'] for g in self.groups.values() 
                          if g.get('platform') == 'slack' and g.get('workspace_id') == workspace_id]
            
            if groups_using:
                await update.message.reply_text(
                    f"Cannot remove workspace '{workspace_id}'. The following groups are using it:\n" +
                    "\n".join(f"  - {name}" for name in groups_using) +
                    "\n\nRemove these groups first using /removegroup"
                )
                return
            
            workspace_name = self.slack_workspaces[workspace_id].get('name', workspace_id)
            
            # Remove client
            if workspace_id in self.slack_clients:
                del self.slack_clients[workspace_id]
            
            # Remove from storage
            del self.slack_workspaces[workspace_id]
            self.save_data()
            
            await update.message.reply_text(f"Removed Slack workspace: {workspace_name} ({workspace_id})")
            logger.info(f"Removed Slack workspace: {workspace_id}")
            
        except Exception as e:
            logger.error(f"Error removing Slack workspace: {e}")
            await update.message.reply_text(f"Error removing Slack workspace: {e}")
    
    async def addgroup(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Add a new group to monitor (Owner only)"""
        chat_id = update.effective_chat.id
        
        if not self.is_owner(chat_id):
            await update.message.reply_text("You don't have permission to use this command. Please contact the bot owner.")
            return
        
        if not context.args or len(context.args) < 1:
            await update.message.reply_text(
                "Usage: /addgroup <platform:identifier> [group_name]\n\n"
                "Platforms: telegram or slack\n\n"
                "Examples:\n"
                "  /addgroup telegram:-1001234567890 Marketing Team\n"
                "  /addgroup slack:workspace_id:C01234ABCDE Sales Channel\n\n"
                "Note:\n"
                "- For Telegram: use the numeric chat ID\n"
                "- For Slack: use workspace_id:channel_id format\n"
                "- Use /listslack to see available workspace IDs"
            )
            return
        
        try:
            # Parse platform:identifier format
            parts = context.args[0].split(':', 1)
            
            if len(parts) == 2:
                platform = parts[0].lower()
                identifier = parts[1]
            else:
                # Backward compatibility: assume telegram if no platform specified
                platform = 'telegram'
                identifier = parts[0]
            
            # Validate platform
            if platform not in ['telegram', 'slack']:
                await update.message.reply_text(f"Invalid platform '{platform}'. Use 'telegram' or 'slack'.")
                return
            
            # Handle Slack format: workspace_id:channel_id
            workspace_id = ''
            if platform == 'slack':
                slack_parts = identifier.split(':', 1)
                if len(slack_parts) != 2:
                    await update.message.reply_text(
                        "Invalid Slack format. Use: slack:workspace_id:channel_id\n\n"
                        "Example: /addgroup slack:myworkspace:C01234ABCDE Channel Name\n\n"
                        "Use /listslack to see available workspace IDs"
                    )
                    return
                
                workspace_id = slack_parts[0].lower()
                channel_id = slack_parts[1]
                
                # Validate workspace exists
                if workspace_id not in self.slack_workspaces:
                    await update.message.reply_text(
                        f"Slack workspace '{workspace_id}' not found.\n\n"
                        f"Use /listslack to see available workspaces or /addslack to add one."
                    )
                    return
            else:
                # Telegram
                channel_id = identifier
            
            # Generate unique group ID
            # For telegram, use the numeric chat_id; for slack, use hash of channel_id
            if platform == 'telegram':
                new_group_id = int(channel_id)
            else:  # slack
                # Use hash to generate unique ID for Slack channels
                new_group_id = hash(f"slack:{channel_id}") & 0x7FFFFFFF  # Ensure positive int
            
            group_name = ' '.join(context.args[1:]) if len(context.args) > 1 else f"{platform.title()} Group"
            
            if new_group_id in self.groups:
                await update.message.reply_text(
                    f"Group {new_group_id} is already being monitored as '{self.groups[new_group_id]['name']}'"
                )
                return
            
            self.groups[new_group_id] = {
                'name': group_name,
                'keywords': set(),
                'subreddits': set(),
                'subreddit_blacklist': set(),
                'enabled': True,
                'platform': platform,
                'channel_id': channel_id,
                'workspace_id': workspace_id  # Empty for telegram, workspace_id for slack
            }
            
            self.save_data()
            
            response_msg = f"Added {platform.title()} group: {group_name}\n"
            response_msg += f"Platform: {platform}\n"
            if platform == 'slack':
                response_msg += f"Workspace: {workspace_id}\n"
            response_msg += f"Channel ID: {channel_id}\n"
            response_msg += f"Internal ID: {new_group_id}\n\n"
            response_msg += f"You can now add keywords for this group using /group command"
            
            await update.message.reply_text(response_msg)
            logger.info(f"Added new {platform} group: {group_name} ({new_group_id})")
            
        except ValueError as e:
            await update.message.reply_text(f"Invalid format. For Telegram, use numeric chat ID. Error: {e}")
        except Exception as e:
            logger.error(f"Error adding group: {e}")
            await update.message.reply_text(f"Error adding group: {e}")
    
    async def removegroup(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Remove a group from monitoring (Owner only)"""
        chat_id = update.effective_chat.id
        
        if not self.is_owner(chat_id):
            await update.message.reply_text("You don't have permission to use this command. Please contact the bot owner.")
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /removegroup <group_chat_id>")
            return
        
        try:
            group_id = int(context.args[0])
            
            if group_id == self.owner_chat_id:
                await update.message.reply_text("Cannot remove the owner's control group")
                return
            
            if group_id not in self.groups:
                await update.message.reply_text(f"Group {group_id} is not being monitored.")
                return
            
            group_name = self.groups[group_id]['name']
            del self.groups[group_id]
            
            if group_id in self.processed_items:
                del self.processed_items[group_id]
            
            # Clean up last_search_time entries for this group
            keys_to_remove = [k for k in self.last_search_time.keys() if k.startswith(f"{group_id}:")]
            for key in keys_to_remove:
                del self.last_search_time[key]
            
            self.save_data()
            await update.message.reply_text(f"Removed group: {group_name} (ID: {group_id})")
            logger.info(f"Removed group: {group_name} ({group_id})")
            
        except ValueError:
            await update.message.reply_text("Invalid group ID. Please provide a valid numeric group chat ID.")
        except Exception as e:
            logger.error(f"Error removing group: {e}")
            await update.message.reply_text(f"Error removing group: {e}")
    
    async def group(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Select a group to manage via interactive menu (Owner only)"""
        chat_id = update.effective_chat.id
        
        if not self.is_owner(chat_id):
            await update.message.reply_text("You don't have permission to use this command. Please contact the bot owner.")
            return
        
        if not self.groups:
            await update.message.reply_text("No groups available. Add a group first using /addgroup")
            return
        
        # Create inline keyboard with all groups
        keyboard = []
        for group_id, group_info in self.groups.items():
            keyword_count = len(group_info['keywords'])
            status_icon = "✓" if group_info['enabled'] else "✗"
            platform = group_info.get('platform', 'telegram')
            platform_icon = "📱" if platform == 'telegram' else "💬"
            button_text = f"{status_icon} {platform_icon} {group_info['name']} ({keyword_count} kw)"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"manage_group:{group_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Select a group to manage:", reply_markup=reply_markup)
    
    async def group_selection_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle group selection and menu interactions"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        data = query.data
        
        # Main group management menu
        if data.startswith("manage_group:"):
            try:
                group_id = int(data.split(":")[1])
                
                if group_id not in self.groups:
                    await query.edit_message_text("Group not found")
                    return
                
                group_info = self.groups[group_id]
                keyword_count = len(group_info['keywords'])
                case_keyword_count = len(group_info.get('case_sensitive_keywords', set()))
                subs = group_info.get('subreddits', set())
                subs_status = f"{len(subs)} subs" if subs else "All subreddits"
                blacklist = group_info.get('subreddit_blacklist', set())
                blacklist_status = f"{len(blacklist)} blocked" if blacklist else "None"
                status = "Enabled" if group_info['enabled'] else "Disabled"
                platform = group_info.get('platform', 'telegram')
                channel_id = group_info.get('channel_id', str(group_id))
                
                # Build menu
                keyboard = [
                    [InlineKeyboardButton("➕ Add Keywords", callback_data=f"add_kw:{group_id}")],
                    [InlineKeyboardButton("➖ Remove Keywords", callback_data=f"remove_kw:{group_id}")],
                    [InlineKeyboardButton("📋 List Keywords", callback_data=f"list_kw:{group_id}")],
                    [InlineKeyboardButton("🗑️ Clear All Keywords", callback_data=f"clear_kw:{group_id}")],
                    [InlineKeyboardButton("🔤 Specify Case", callback_data=f"case_menu:{group_id}")],
                    [InlineKeyboardButton("➕ Add Subreddit", callback_data=f"add_sr:{group_id}")],
                    [InlineKeyboardButton("➖ Remove Subreddit", callback_data=f"remove_sr:{group_id}")],
                    [InlineKeyboardButton("📋 List Subreddits", callback_data=f"list_sr:{group_id}")],
                    [InlineKeyboardButton("🗑️ Clear Subreddits (All)", callback_data=f"clear_sr:{group_id}")],
                    [InlineKeyboardButton("🚫 Blacklist Subreddits", callback_data=f"blacklist_menu:{group_id}")],
                    [InlineKeyboardButton(f"🔄 Toggle ({status})", callback_data=f"toggle:{group_id}")],
                    [InlineKeyboardButton("« Back to Groups", callback_data="back_to_groups")]
                ]
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                message = f"Managing: {group_info['name']}\n\n"
                message += f"Platform: {platform.title()}\n"
                message += f"Channel ID: {channel_id}\n"
                message += f"Status: {status}\n"
                message += f"Keywords: {keyword_count}\n"
                message += f"Case-Sensitive: {case_keyword_count}\n"
                message += f"Subreddits: {subs_status}\n"
                message += f"Blacklist: {blacklist_status}\n"
                message += f"Internal ID: {group_id}"
                
                await query.edit_message_text(message, reply_markup=reply_markup)
                
            except (ValueError, IndexError) as e:
                logger.error(f"Error parsing callback data: {e}")
                await query.edit_message_text("Error processing selection")
        
        # Add keywords flow
        elif data.startswith("add_kw:"):
            group_id = int(data.split(":")[1])
            self.pending_keyword_add[user_id] = group_id
            self.menu_state[user_id] = "adding_keywords"
            
            group_name = self.groups[group_id]['name']
            current_keywords = self.groups[group_id]['keywords']
            
            keywords_text = "\n  ".join(sorted(current_keywords)) if current_keywords else "None"
            
            keyboard = [[InlineKeyboardButton("« Cancel", callback_data=f"manage_group:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"Adding keywords to: {group_name}\n\n"
                f"Current keywords:\n  {keywords_text}\n\n"
                f"Send your keywords separated by commas:\n"
                f"Example: pain killer, mutual fund, crypto news",
                reply_markup=reply_markup
            )
        
        # Remove keywords flow
        elif data.startswith("remove_kw:"):
            group_id = int(data.split(":")[1])
            
            if not self.groups[group_id]['keywords']:
                keyboard = [[InlineKeyboardButton("« Back", callback_data=f"manage_group:{group_id}")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(
                    f"No keywords to remove from {self.groups[group_id]['name']}",
                    reply_markup=reply_markup
                )
                return
            
            self.pending_keyword_remove[user_id] = group_id
            self.menu_state[user_id] = "removing_keywords"
            
            group_name = self.groups[group_id]['name']
            current_keywords = sorted(self.groups[group_id]['keywords'])
            keywords_text = "\n  ".join(current_keywords)
            
            keyboard = [[InlineKeyboardButton("« Cancel", callback_data=f"manage_group:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"Removing keywords from: {group_name}\n\n"
                f"Current keywords:\n  {keywords_text}\n\n"
                f"Send keywords to remove (comma-separated):",
                reply_markup=reply_markup
            )

        # Add subreddit flow
        elif data.startswith("add_sr:"):
            group_id = int(data.split(":")[1])
            self.pending_subreddit_add[user_id] = group_id
            self.menu_state[user_id] = "adding_subs"

            group_name = self.groups[group_id]['name']
            current_subs = self.groups[group_id].get('subreddits', set())
            subs_text = "\n  ".join(sorted(current_subs)) if current_subs else "All (no filter)"

            keyboard = [[InlineKeyboardButton("« Cancel", callback_data=f"manage_group:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                f"Adding subreddits to: {group_name}\n\n"
                f"Current subreddits:\n  {subs_text}\n\n"
                f"Send subreddit names separated by commas:\n"
                f"Example: wallstreetbets, stocks, cryptoCurrency\n\n"
                f"Tip: You can include or omit the r/ prefix.",
                reply_markup=reply_markup
            )

        # Remove subreddit flow
        elif data.startswith("remove_sr:"):
            group_id = int(data.split(":")[1])
            subs = self.groups[group_id].get('subreddits', set())
            if not subs:
                keyboard = [[InlineKeyboardButton("« Back", callback_data=f"manage_group:{group_id}")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(
                    f"No subreddit filter configured for {self.groups[group_id]['name']} (currently All).",
                    reply_markup=reply_markup
                )
                return

            self.pending_subreddit_remove[user_id] = group_id
            self.menu_state[user_id] = "removing_subs"

            group_name = self.groups[group_id]['name']
            subs_text = "\n  ".join(sorted(subs))

            keyboard = [[InlineKeyboardButton("« Cancel", callback_data=f"manage_group:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                f"Removing subreddits from: {group_name}\n\n"
                f"Current subreddits:\n  {subs_text}\n\n"
                f"Send subreddits to remove (comma-separated):",
                reply_markup=reply_markup
            )

        # List subreddits
        elif data.startswith("list_sr:"):
            group_id = int(data.split(":")[1])
            subs = sorted(self.groups[group_id].get('subreddits', set()))

            if not subs:
                subs_text = "All (no filter)"
            else:
                subs_text = "\n  ".join(subs)

            keyboard = [[InlineKeyboardButton("« Back", callback_data=f"manage_group:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            message = f"{self.groups[group_id]['name']}\n\n"
            message += f"Subreddits ({'All' if not subs else len(subs)}):\n  {subs_text}"

            await query.edit_message_text(message, reply_markup=reply_markup)

        # Clear subreddit filter (revert to All)
        elif data.startswith("clear_sr:"):
            group_id = int(data.split(":")[1])
            count = len(self.groups[group_id].get('subreddits', set()))
            self.groups[group_id]['subreddits'] = set()
            self.save_data()

            keyboard = [[InlineKeyboardButton("« Back", callback_data=f"manage_group:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                f"Cleared subreddit filter ({count} removed). Now monitoring All subreddits.",
                reply_markup=reply_markup
            )
            logger.info(f"Cleared subreddit filter from group {group_id}")

        # Blacklist management menu
        elif data.startswith("blacklist_menu:"):
            group_id = int(data.split(":")[1])
            blacklist = sorted(self.groups[group_id].get('subreddit_blacklist', set()))
            count = len(blacklist)

            keyboard = [
                [InlineKeyboardButton("➕ Add to Blacklist", callback_data=f"add_bl:{group_id}")],
                [InlineKeyboardButton("➖ Remove from Blacklist", callback_data=f"remove_bl:{group_id}")],
                [InlineKeyboardButton("📋 List Blacklisted", callback_data=f"list_bl:{group_id}")],
                [InlineKeyboardButton("🗑️ Clear Blacklist", callback_data=f"clear_bl:{group_id}")],
                [InlineKeyboardButton("« Back", callback_data=f"manage_group:{group_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            message = f"Blacklist for {self.groups[group_id]['name']}:\n\n"
            if count == 0:
                message += "No subreddits are currently blacklisted.\n\n"
            else:
                formatted = '\n  '.join(blacklist)
                message += f"Blacklisted ({count}):\n  {formatted}\n\n"
            message += "Choose an action below."

            await query.edit_message_text(message, reply_markup=reply_markup)

        # Add to blacklist flow
        elif data.startswith("add_bl:"):
            group_id = int(data.split(":")[1])
            self.pending_subreddit_blacklist_add[user_id] = group_id
            self.menu_state[user_id] = "adding_blacklist"

            current = self.groups[group_id].get('subreddit_blacklist', set())
            current_text = "\n  ".join(sorted(current)) if current else "None"

            keyboard = [[InlineKeyboardButton("« Cancel", callback_data=f"blacklist_menu:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                f"Blacklist Subreddits for: {self.groups[group_id]['name']}\n\n"
                f"Currently blacklisted:\n  {current_text}\n\n"
                f"Send subreddit names to blacklist (comma-separated).\n"
                f"Example: wallstreetbets, stocks\n\n"
                f"Tip: r/ prefix optional.",
                reply_markup=reply_markup
            )

        # Remove from blacklist flow
        elif data.startswith("remove_bl:"):
            group_id = int(data.split(":")[1])
            current = self.groups[group_id].get('subreddit_blacklist', set())

            if not current:
                keyboard = [[InlineKeyboardButton("« Back", callback_data=f"blacklist_menu:{group_id}")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(
                    "No subreddits are blacklisted for this group.",
                    reply_markup=reply_markup
                )
                return

            self.pending_subreddit_blacklist_remove[user_id] = group_id
            self.menu_state[user_id] = "removing_blacklist"

            current_text = "\n  ".join(sorted(current))
            keyboard = [[InlineKeyboardButton("« Cancel", callback_data=f"blacklist_menu:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                f"Removing from blacklist: {self.groups[group_id]['name']}\n\n"
                f"Current blacklist:\n  {current_text}\n\n"
                f"Send subreddit names to remove (comma-separated).",
                reply_markup=reply_markup
            )

        # List blacklisted subreddits
        elif data.startswith("list_bl:"):
            group_id = int(data.split(":")[1])
            blacklist = sorted(self.groups[group_id].get('subreddit_blacklist', set()))

            if not blacklist:
                content = "No subreddits are blacklisted (monitoring all unless whitelisted)."
            else:
                content = "\n  ".join(blacklist)

            keyboard = [[InlineKeyboardButton("« Back", callback_data=f"blacklist_menu:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            message = f"{self.groups[group_id]['name']}\n\n"
            message += f"Blacklisted Subreddits ({len(blacklist)}):\n  {content}"

            await query.edit_message_text(message, reply_markup=reply_markup)

        # Clear blacklist
        elif data.startswith("clear_bl:"):
            group_id = int(data.split(":")[1])
            count = len(self.groups[group_id].get('subreddit_blacklist', set()))
            self.groups[group_id]['subreddit_blacklist'] = set()
            self.save_data()

            keyboard = [[InlineKeyboardButton("« Back", callback_data=f"blacklist_menu:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                f"Cleared {count} subreddits from blacklist. All allowed unless whitelisted.",
                reply_markup=reply_markup
            )
            logger.info(f"Cleared subreddit blacklist for group {group_id}")
        
        # List keywords
        elif data.startswith("list_kw:"):
            group_id = int(data.split(":")[1])
            group_info = self.groups[group_id]
            keywords = sorted(group_info['keywords'])
            
            if not keywords:
                keywords_text = "No keywords configured"
            else:
                keywords_text = "\n  ".join(keywords)
            
            keyboard = [[InlineKeyboardButton("« Back", callback_data=f"manage_group:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            message = f"{group_info['name']}\n\n"
            message += f"Keywords ({len(keywords)}):\n  {keywords_text}"
            
            await query.edit_message_text(message, reply_markup=reply_markup)
        
        # Clear all keywords
        elif data.startswith("clear_kw:"):
            group_id = int(data.split(":")[1])
            
            keyboard = [
                [InlineKeyboardButton("✓ Yes, Clear All", callback_data=f"confirm_clear:{group_id}")],
                [InlineKeyboardButton("✗ Cancel", callback_data=f"manage_group:{group_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            count = len(self.groups[group_id]['keywords'])
            await query.edit_message_text(
                f"Are you sure you want to clear all {count} keywords from {self.groups[group_id]['name']}?",
                reply_markup=reply_markup
            )
        
        # Confirm clear
        elif data.startswith("confirm_clear:"):
            group_id = int(data.split(":")[1])
            count = len(self.groups[group_id]['keywords'])
            self.groups[group_id]['keywords'].clear()
            self.save_data()
            
            keyboard = [[InlineKeyboardButton("« Back", callback_data=f"manage_group:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"Cleared {count} keywords from {self.groups[group_id]['name']}",
                reply_markup=reply_markup
            )
            logger.info(f"Cleared {count} keywords from group {group_id}")
        
        # Case-sensitive keywords menu
        elif data.startswith("case_menu:"):
            group_id = int(data.split(":")[1])
            group_info = self.groups[group_id]
            case_keywords = group_info.get('case_sensitive_keywords', set())
            count = len(case_keywords)
            
            keyboard = [
                [InlineKeyboardButton("➕ Add Case-Sensitive Keyword", callback_data=f"add_case_kw:{group_id}")],
                [InlineKeyboardButton("➖ Remove Case-Sensitive Keyword", callback_data=f"remove_case_kw:{group_id}")],
                [InlineKeyboardButton("📋 List Case-Sensitive Keywords", callback_data=f"list_case_kw:{group_id}")],
                [InlineKeyboardButton("🗑️ Clear All Case-Sensitive Keywords", callback_data=f"clear_case_kw:{group_id}")],
                [InlineKeyboardButton("« Back", callback_data=f"manage_group:{group_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            message = f"Case-Sensitive Keywords for: {group_info['name']}\n\n"
            if count == 0:
                message += "No case-sensitive keywords configured.\n\n"
            else:
                formatted = '\n  '.join(sorted(case_keywords))
                message += f"Case-Sensitive Keywords ({count}):\n  {formatted}\n\n"
            message += "Case-sensitive keywords match exactly as typed (e.g., 'CdQ' only matches 'CdQ', not 'cdq' or 'CDQ')."
            
            await query.edit_message_text(message, reply_markup=reply_markup)
        
        # Add case-sensitive keyword flow
        elif data.startswith("add_case_kw:"):
            group_id = int(data.split(":")[1])
            self.pending_case_keyword_add[user_id] = group_id
            self.menu_state[user_id] = "adding_case_keywords"
            
            group_name = self.groups[group_id]['name']
            current_keywords = self.groups[group_id].get('case_sensitive_keywords', set())
            
            keywords_text = "\n  ".join(sorted(current_keywords)) if current_keywords else "None"
            
            keyboard = [[InlineKeyboardButton("« Cancel", callback_data=f"case_menu:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"Adding case-sensitive keywords to: {group_name}\n\n"
                f"Current case-sensitive keywords:\n  {keywords_text}\n\n"
                f"Send keywords with exact case you want to match:\n"
                f"Example: CdQ, CDQ, Tesla (will only match exactly as typed)\n\n"
                f"Separate multiple keywords with commas.",
                reply_markup=reply_markup
            )
        
        # Remove case-sensitive keyword flow
        elif data.startswith("remove_case_kw:"):
            group_id = int(data.split(":")[1])
            case_keywords = self.groups[group_id].get('case_sensitive_keywords', set())
            
            if not case_keywords:
                keyboard = [[InlineKeyboardButton("« Back", callback_data=f"case_menu:{group_id}")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(
                    f"No case-sensitive keywords to remove from {self.groups[group_id]['name']}",
                    reply_markup=reply_markup
                )
                return
            
            self.pending_case_keyword_remove[user_id] = group_id
            self.menu_state[user_id] = "removing_case_keywords"
            
            group_name = self.groups[group_id]['name']
            current_keywords = sorted(case_keywords)
            keywords_text = "\n  ".join(current_keywords)
            
            keyboard = [[InlineKeyboardButton("« Cancel", callback_data=f"case_menu:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"Removing case-sensitive keywords from: {group_name}\n\n"
                f"Current case-sensitive keywords:\n  {keywords_text}\n\n"
                f"Send keywords to remove (comma-separated, match exact case):\n"
                f"Example: CdQ, CDQ",
                reply_markup=reply_markup
            )
        
        # List case-sensitive keywords
        elif data.startswith("list_case_kw:"):
            group_id = int(data.split(":")[1])
            case_keywords = sorted(self.groups[group_id].get('case_sensitive_keywords', set()))
            
            if not case_keywords:
                content = "No case-sensitive keywords configured."
            else:
                content = "\n  ".join(case_keywords)
            
            keyboard = [[InlineKeyboardButton("« Back", callback_data=f"case_menu:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            message = f"{self.groups[group_id]['name']}\n\n"
            message += f"Case-Sensitive Keywords ({len(case_keywords)}):\n  {content}"
            
            await query.edit_message_text(message, reply_markup=reply_markup)
        
        # Clear case-sensitive keywords
        elif data.startswith("clear_case_kw:"):
            group_id = int(data.split(":")[1])
            case_keywords = self.groups[group_id].get('case_sensitive_keywords', set())
            count = len(case_keywords)
            
            if count == 0:
                keyboard = [[InlineKeyboardButton("« Back", callback_data=f"case_menu:{group_id}")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(
                    f"No case-sensitive keywords to clear in {self.groups[group_id]['name']}",
                    reply_markup=reply_markup
                )
                return
            
            self.groups[group_id]['case_sensitive_keywords'] = set()
            self.save_data()
            
            keyboard = [[InlineKeyboardButton("« Back", callback_data=f"case_menu:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"Cleared {count} case-sensitive keywords from {self.groups[group_id]['name']}",
                reply_markup=reply_markup
            )
            logger.info(f"Cleared {count} case-sensitive keywords from group {group_id}")
        
        # Toggle group
        elif data.startswith("toggle:"):
            group_id = int(data.split(":")[1])
            self.groups[group_id]['enabled'] = not self.groups[group_id]['enabled']
            status = "enabled" if self.groups[group_id]['enabled'] else "disabled"
            self.save_data()
            
            keyboard = [[InlineKeyboardButton("« Back", callback_data=f"manage_group:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"Group '{self.groups[group_id]['name']}' is now {status}",
                reply_markup=reply_markup
            )
            logger.info(f"Group {group_id} {status}")
        
        # Back to groups list
        elif data == "back_to_groups":
            keyboard = []
            for group_id, group_info in self.groups.items():
                keyword_count = len(group_info['keywords'])
                status_icon = "✓" if group_info['enabled'] else "✗"
                platform = group_info.get('platform', 'telegram')
                platform_icon = "📱" if platform == 'telegram' else "💬"
                button_text = f"{status_icon} {platform_icon} {group_info['name']} ({keyword_count} kw)"
                keyboard.append([InlineKeyboardButton(button_text, callback_data=f"manage_group:{group_id}")])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("Select a group to manage:", reply_markup=reply_markup)
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle text messages for adding/removing keywords after menu selection"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Only process in owner's group
        if not self.is_owner(chat_id):
            return
        
        # Check menu state
        menu_state = self.menu_state.get(user_id)
        
        # Adding keywords
        if user_id in self.pending_keyword_add and menu_state == "adding_keywords":
            group_id = self.pending_keyword_add[user_id]
            
            # Parse comma-separated keywords
            text = update.message.text
            keywords = [kw.strip().lower() for kw in text.split(',') if kw.strip()]
            
            if not keywords:
                await update.message.reply_text("No valid keywords found. Please try again.")
                return
            
            # Add keywords to the selected group
            added = []
            skipped = []
            
            for keyword in keywords:
                if keyword in self.groups[group_id]['keywords']:
                    skipped.append(keyword)
                else:
                    self.groups[group_id]['keywords'].add(keyword)
                    added.append(keyword)
            
            # Clear pending state
            del self.pending_keyword_add[user_id]
            del self.menu_state[user_id]
            
            self.save_data()
            
            # Format response with back button
            response = f"Keywords added to '{self.groups[group_id]['name']}':\n\n"
            
            if added:
                response += "Added:\n  " + "\n  ".join(added)
            
            if skipped:
                response += "\n\nSkipped (already exists):\n  " + "\n  ".join(skipped)
            
            keyboard = [[InlineKeyboardButton("« Back to Group", callback_data=f"manage_group:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(response, reply_markup=reply_markup)
            logger.info(f"Added {len(added)} keywords to group {group_id}")
        
        # Removing keywords
        elif user_id in self.pending_keyword_remove and menu_state == "removing_keywords":
            group_id = self.pending_keyword_remove[user_id]
            
            # Parse comma-separated keywords
            text = update.message.text
            keywords = [kw.strip().lower() for kw in text.split(',') if kw.strip()]
            
            if not keywords:
                await update.message.reply_text("No valid keywords found. Please try again.")
                return
            
            # Remove keywords from the selected group
            removed = []
            not_found = []
            
            for keyword in keywords:
                if keyword in self.groups[group_id]['keywords']:
                    self.groups[group_id]['keywords'].remove(keyword)
                    removed.append(keyword)
                else:
                    not_found.append(keyword)
            
            # Clear pending state
            del self.pending_keyword_remove[user_id]
            del self.menu_state[user_id]
            
            self.save_data()
            
            # Format response with back button
            response = f"Keywords removed from '{self.groups[group_id]['name']}':\n\n"
            
            if removed:
                response += "Removed:\n  " + "\n  ".join(removed)
            
            if not_found:
                response += "\n\nNot found:\n  " + "\n  ".join(not_found)
            
            keyboard = [[InlineKeyboardButton("« Back to Group", callback_data=f"manage_group:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(response, reply_markup=reply_markup)
            logger.info(f"Removed {len(removed)} keywords from group {group_id}")

        # Adding subreddits
        elif user_id in self.pending_subreddit_add and menu_state == "adding_subs":
            group_id = self.pending_subreddit_add[user_id]

            text = update.message.text
            # Normalize subreddit names: strip r/ and whitespace, lowercase
            subs = []
            for token in text.split(','):
                name = token.strip().lower()
                if not name:
                    continue
                if name.startswith('r/'):
                    name = name[2:]
                # basic validation: alphanum + underscores is typical
                name = re.sub(r'[^a-z0-9_]+', '', name)
                if name:
                    subs.append(name)

            if not subs:
                await update.message.reply_text("No valid subreddit names found. Please try again.")
                return

            if 'subreddits' not in self.groups[group_id]:
                self.groups[group_id]['subreddits'] = set()

            added = []
            skipped = []
            for s in subs:
                if s in self.groups[group_id]['subreddits']:
                    skipped.append(s)
                else:
                    self.groups[group_id]['subreddits'].add(s)
                    added.append(s)

            # Clear pending state
            del self.pending_subreddit_add[user_id]
            del self.menu_state[user_id]

            self.save_data()

            response = f"Subreddits added to '{self.groups[group_id]['name']}':\n\n"
            if added:
                response += "Added:\n  " + "\n  ".join(added)
            if skipped:
                response += "\n\nSkipped (already exists):\n  " + "\n  ".join(skipped)

            keyboard = [[InlineKeyboardButton("« Back to Group", callback_data=f"manage_group:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(response, reply_markup=reply_markup)
            logger.info(f"Added {len(added)} subreddits to group {group_id}")

        # Removing subreddits
        elif user_id in self.pending_subreddit_remove and menu_state == "removing_subs":
            group_id = self.pending_subreddit_remove[user_id]

            text = update.message.text
            subs = []
            for token in text.split(','):
                name = token.strip().lower()
                if not name:
                    continue
                if name.startswith('r/'):
                    name = name[2:]
                name = re.sub(r'[^a-z0-9_]+', '', name)
                if name:
                    subs.append(name)

            if not subs:
                await update.message.reply_text("No valid subreddit names found. Please try again.")
                return

            if 'subreddits' not in self.groups[group_id] or not self.groups[group_id]['subreddits']:
                await update.message.reply_text("No subreddit filter configured for this group.")
                return

            removed = []
            not_found = []
            for s in subs:
                if s in self.groups[group_id]['subreddits']:
                    self.groups[group_id]['subreddits'].remove(s)
                    removed.append(s)
                else:
                    not_found.append(s)

            # If set becomes empty, it means 'All'
            if not self.groups[group_id]['subreddits']:
                self.groups[group_id]['subreddits'] = set()

            # Clear pending state
            del self.pending_subreddit_remove[user_id]
            del self.menu_state[user_id]

            self.save_data()

            response = f"Subreddits updated for '{self.groups[group_id]['name']}':\n\n"
            if removed:
                response += "Removed:\n  " + "\n  ".join(removed)
            if not_found:
                response += "\n\nNot found:\n  " + "\n  ".join(not_found)

            keyboard = [[InlineKeyboardButton("« Back to Group", callback_data=f"manage_group:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(response, reply_markup=reply_markup)
            logger.info(f"Removed {len(removed)} subreddits from group {group_id}")
    
        # Adding case-sensitive keywords
        elif user_id in self.pending_case_keyword_add and menu_state == "adding_case_keywords":
            group_id = self.pending_case_keyword_add[user_id]
            
            text = update.message.text
            keywords = [kw.strip() for kw in text.split(',') if kw.strip()]  # Keep original case!
            
            if not keywords:
                await update.message.reply_text("No valid keywords provided. Please try again.")
                return
            
            if 'case_sensitive_keywords' not in self.groups[group_id]:
                self.groups[group_id]['case_sensitive_keywords'] = set()
            
            added = []
            skipped = []
            for keyword in keywords:
                if keyword in self.groups[group_id]['case_sensitive_keywords']:
                    skipped.append(keyword)
                else:
                    self.groups[group_id]['case_sensitive_keywords'].add(keyword)
                    added.append(keyword)
            
            # Clear pending state
            del self.pending_case_keyword_add[user_id]
            del self.menu_state[user_id]
            
            self.save_data()
            
            response = f"Case-sensitive keywords added to '{self.groups[group_id]['name']}':\n\n"
            if added:
                response += "Added:\n  " + "\n  ".join(added)
            if skipped:
                response += "\n\nSkipped (already exists):\n  " + "\n  ".join(skipped)
            
            keyboard = [[InlineKeyboardButton("« Back to Menu", callback_data=f"case_menu:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(response, reply_markup=reply_markup)
            logger.info(f"Added {len(added)} case-sensitive keywords to group {group_id}")
        
        # Removing case-sensitive keywords
        elif user_id in self.pending_case_keyword_remove and menu_state == "removing_case_keywords":
            group_id = self.pending_case_keyword_remove[user_id]
            
            text = update.message.text
            keywords = [kw.strip() for kw in text.split(',') if kw.strip()]  # Keep original case!
            
            if not keywords:
                await update.message.reply_text("No valid keywords provided. Please try again.")
                return
            
            if 'case_sensitive_keywords' not in self.groups[group_id] or not self.groups[group_id]['case_sensitive_keywords']:
                await update.message.reply_text("No case-sensitive keywords configured for this group.")
                return
            
            removed = []
            not_found = []
            for keyword in keywords:
                if keyword in self.groups[group_id]['case_sensitive_keywords']:
                    self.groups[group_id]['case_sensitive_keywords'].remove(keyword)
                    removed.append(keyword)
                else:
                    not_found.append(keyword)
            
            # Clear pending state
            del self.pending_case_keyword_remove[user_id]
            del self.menu_state[user_id]
            
            self.save_data()
            
            response = f"Case-sensitive keywords updated for '{self.groups[group_id]['name']}':\n\n"
            if removed:
                response += "Removed:\n  " + "\n  ".join(removed)
            if not_found:
                response += "\n\nNot found:\n  " + "\n  ".join(not_found)
            
            keyboard = [[InlineKeyboardButton("« Back to Menu", callback_data=f"case_menu:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(response, reply_markup=reply_markup)
            logger.info(f"Removed {len(removed)} case-sensitive keywords from group {group_id}")
        
        # Adding blacklist subreddits
        elif user_id in self.pending_subreddit_blacklist_add and menu_state == "adding_blacklist":
            group_id = self.pending_subreddit_blacklist_add[user_id]

            text = update.message.text
            subs = []
            for token in text.split(','):
                name = token.strip().lower()
                if not name:
                    continue
                if name.startswith('r/'):
                    name = name[2:]
                name = re.sub(r'[^a-z0-9_]+', '', name)
                if name:
                    subs.append(name)

            if not subs:
                await update.message.reply_text("No valid subreddit names found. Please try again.")
                return

            if 'subreddit_blacklist' not in self.groups[group_id]:
                self.groups[group_id]['subreddit_blacklist'] = set()

            added = []
            skipped = []
            for s in subs:
                if s in self.groups[group_id]['subreddit_blacklist']:
                    skipped.append(s)
                else:
                    self.groups[group_id]['subreddit_blacklist'].add(s)
                    added.append(s)
                    # Also ensure whitelist doesn't include it if both were set
                    if s in self.groups[group_id].get('subreddits', set()):
                        self.groups[group_id]['subreddits'].discard(s)

            # Clear pending state
            del self.pending_subreddit_blacklist_add[user_id]
            del self.menu_state[user_id]

            self.save_data()

            response = f"Subreddit blacklist updated for '{self.groups[group_id]['name']}':\n\n"
            if added:
                response += "Blacklisted:\n  " + "\n  ".join(added)
            if skipped:
                response += "\n\nSkipped (already blacklisted):\n  " + "\n  ".join(skipped)

            keyboard = [[InlineKeyboardButton("« Back", callback_data=f"blacklist_menu:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(response, reply_markup=reply_markup)
            logger.info(f"Added {len(added)} subreddits to blacklist for group {group_id}")

        # Removing blacklist subreddits
        elif user_id in self.pending_subreddit_blacklist_remove and menu_state == "removing_blacklist":
            group_id = self.pending_subreddit_blacklist_remove[user_id]

            text = update.message.text
            subs = []
            for token in text.split(','):
                name = token.strip().lower()
                if not name:
                    continue
                if name.startswith('r/'):
                    name = name[2:]
                name = re.sub(r'[^a-z0-9_]+', '', name)
                if name:
                    subs.append(name)

            if not subs:
                await update.message.reply_text("No valid subreddit names found. Please try again.")
                return

            blacklist = self.groups[group_id].get('subreddit_blacklist', set())
            if not blacklist:
                await update.message.reply_text("No subreddits are blacklisted for this group.")
                return

            removed = []
            not_found = []
            for s in subs:
                if s in blacklist:
                    blacklist.remove(s)
                    removed.append(s)
                else:
                    not_found.append(s)

            # Clear pending state
            del self.pending_subreddit_blacklist_remove[user_id]
            del self.menu_state[user_id]

            self.save_data()

            response = f"Subreddit blacklist updated for '{self.groups[group_id]['name']}':\n\n"
            if removed:
                response += "Removed:\n  " + "\n  ".join(removed)
            if not_found:
                response += "\n\nNot found:\n  " + "\n  ".join(not_found)

            keyboard = [[InlineKeyboardButton("« Back", callback_data=f"blacklist_menu:{group_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(response, reply_markup=reply_markup)
            logger.info(f"Removed {len(removed)} subreddits from blacklist for group {group_id}")

    async def addkeyword(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Add keywords to a group via direct command (Owner only)"""
        chat_id = update.effective_chat.id
        
        if not self.is_owner(chat_id):
            await update.message.reply_text("You don't have permission to use this command. Please contact the bot owner.")
            return
        
        if not context.args or len(context.args) < 2:
            await update.message.reply_text(
                "Usage: /addkeyword <group_id> <keyword1>, <keyword2>, ...\n"
                "Example: /addkeyword -1001234567890 pain killer, mutual fund\n\n"
                "Tip: You can also use /group menu for interactive keyword management"
            )
            return
        
        try:
            group_id = int(context.args[0])
            keywords_text = ' '.join(context.args[1:])
            keywords = [kw.strip().lower() for kw in keywords_text.split(',') if kw.strip()]
            
            if group_id not in self.groups:
                await update.message.reply_text(f"Group {group_id} not found. Use /listgroups to see available groups.")
                return
            
            if not keywords:
                await update.message.reply_text("No valid keywords provided.")
                return
            
            added = []
            skipped = []
            
            for keyword in keywords:
                if keyword in self.groups[group_id]['keywords']:
                    skipped.append(keyword)
                else:
                    self.groups[group_id]['keywords'].add(keyword)
                    added.append(keyword)
            
            self.save_data()
            
            response = f"Keywords added to '{self.groups[group_id]['name']}':\n\n"
            
            if added:
                response += "Added:\n  " + "\n  ".join(added)
            
            if skipped:
                response += "\n\nSkipped (already exists):\n  " + "\n  ".join(skipped)
            
            await update.message.reply_text(response)
            logger.info(f"Added {len(added)} keywords to group {group_id} via direct command")
            
        except ValueError:
            await update.message.reply_text("Invalid group ID. Please provide a valid numeric group chat ID.")
        except Exception as e:
            logger.error(f"Error adding keywords: {e}")
            await update.message.reply_text(f"Error adding keywords: {e}")
    
    async def remove_keyword_cmd(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Remove keywords from a group via direct command (Owner only)"""
        chat_id = update.effective_chat.id
        
        if not self.is_owner(chat_id):
            await update.message.reply_text("You don't have permission to use this command. Please contact the bot owner.")
            return
        
        if not context.args or len(context.args) < 2:
            await update.message.reply_text(
                "Usage: /removekeyword <group_id> <keyword1>, <keyword2>, ...\n"
                "Example: /removekeyword -1001234567890 pain killer, crypto\n\n"
                "Tip: You can also use /group menu for interactive keyword management"
            )
            return
        
        try:
            group_id = int(context.args[0])
            keywords_text = ' '.join(context.args[1:])
            keywords = [kw.strip().lower() for kw in keywords_text.split(',') if kw.strip()]
            
            if group_id not in self.groups:
                await update.message.reply_text(f"Group {group_id} not found.")
                return
            
            removed = []
            not_found = []
            
            for keyword in keywords:
                if keyword in self.groups[group_id]['keywords']:
                    self.groups[group_id]['keywords'].remove(keyword)
                    removed.append(keyword)
                else:
                    not_found.append(keyword)
            
            self.save_data()
            
            response = f"Keywords removed from '{self.groups[group_id]['name']}':\n\n"
            
            if removed:
                response += "Removed:\n" + "\n".join(f"  {kw}" for kw in removed)
            
            if not_found:
                response += "\n\nNot found:\n" + "\n".join(f"  {kw}" for kw in not_found)
            
            await update.message.reply_text(response)
            logger.info(f"Removed {len(removed)} keywords from group {group_id}")
            
        except ValueError:
            await update.message.reply_text("Invalid group ID. Please provide a valid numeric group chat ID.")
        except Exception as e:
            logger.error(f"Error removing keywords: {e}")
            await update.message.reply_text(f"Error removing keywords: {e}")
    
    async def listgroups(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List all monitored groups (Owner only)"""
        chat_id = update.effective_chat.id
        
        if not self.is_owner(chat_id):
            await update.message.reply_text("You don't have permission to use this command. Please contact the bot owner.")
            return
        
        if not self.groups:
            await update.message.reply_text("No groups being monitored.")
            return
        
        message = "Monitored Groups:\n\n"
        
        for group_id, group_info in self.groups.items():
            status = "[Active]" if group_info['enabled'] else "[Disabled]"
            keyword_count = len(group_info['keywords'])
            subs = group_info.get('subreddits', set())
            subs_text = "All" if not subs else f"{len(subs)}"
            blacklist = group_info.get('subreddit_blacklist', set())
            blacklist_text = "0" if not blacklist else f"{len(blacklist)}"
            platform = group_info.get('platform', 'telegram')
            channel_id = group_info.get('channel_id', str(group_id))
            message += f"{status} {group_info['name']}\n"
            message += f"   Platform: {platform.title()}\n"
            message += f"   Channel ID: {channel_id}\n"
            message += f"   Internal ID: {group_id}\n"
            message += f"   Keywords: {keyword_count}\n"
            message += f"   Subreddits: {subs_text}\n"
            message += f"   Blacklist: {blacklist_text}\n\n"
        
        await update.message.reply_text(message)
    
    async def listkeywords(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List keywords for a specific group (Owner only)"""
        chat_id = update.effective_chat.id
        
        if not self.is_owner(chat_id):
            await update.message.reply_text("You don't have permission to use this command. Please contact the bot owner.")
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /listkeywords <group_id>")
            return
        
        try:
            group_id = int(context.args[0])
            
            if group_id not in self.groups:
                await update.message.reply_text(f"Group {group_id} not found.")
                return
            
            group_info = self.groups[group_id]
            keywords = sorted(group_info['keywords'])
            
            if not keywords:
                await update.message.reply_text(f"{group_info['name']}\n\nNo keywords configured.")
                return
            
            keywords_text = "\n".join(f"  {kw}" for kw in keywords)
            message = f"{group_info['name']}\n\n"
            message += f"Keywords ({len(keywords)}):\n{keywords_text}"
            
            await update.message.reply_text(message)
            
        except ValueError:
            await update.message.reply_text("Invalid group ID. Please provide a valid numeric group chat ID.")
        except Exception as e:
            logger.error(f"Error listing keywords: {e}")
            await update.message.reply_text(f"Error listing keywords: {e}")
    
    async def cleargroup(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Clear all keywords from a group (Owner only)"""
        chat_id = update.effective_chat.id
        
        if not self.is_owner(chat_id):
            await update.message.reply_text("You don't have permission to use this command. Please contact the bot owner.")
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /cleargroup <group_id>")
            return
        
        try:
            group_id = int(context.args[0])
            
            if group_id not in self.groups:
                await update.message.reply_text(f"Group {group_id} not found.")
                return
            
            count = len(self.groups[group_id]['keywords'])
            self.groups[group_id]['keywords'].clear()
            self.save_data()
            
            await update.message.reply_text(f"Cleared {count} keywords from '{self.groups[group_id]['name']}'")
            logger.info(f"Cleared {count} keywords from group {group_id}")
            
        except ValueError:
            await update.message.reply_text("Invalid group ID. Please provide a valid numeric group chat ID.")
        except Exception as e:
            logger.error(f"Error clearing group keywords: {e}")
            await update.message.reply_text(f"Error clearing group keywords: {e}")
    
    async def togglegroup(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enable or disable a group (Owner only)"""
        chat_id = update.effective_chat.id
        
        if not self.is_owner(chat_id):
            await update.message.reply_text("You don't have permission to use this command. Please contact the bot owner.")
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /togglegroup <group_id>")
            return
        
        try:
            group_id = int(context.args[0])
            
            if group_id not in self.groups:
                await update.message.reply_text(f"Group {group_id} not found.")
                return
            
            self.groups[group_id]['enabled'] = not self.groups[group_id]['enabled']
            status = "enabled" if self.groups[group_id]['enabled'] else "disabled"
            
            self.save_data()
            
            await update.message.reply_text(f"Group '{self.groups[group_id]['name']}' is now {status}")
            logger.info(f"Group {group_id} {status}")
            
        except ValueError:
            await update.message.reply_text("Invalid group ID. Please provide a valid numeric group chat ID.")
        except Exception as e:
            logger.error(f"Error toggling group: {e}")
            await update.message.reply_text(f"Error toggling group: {e}")
    
    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show bot status (Owner only)"""
        chat_id = update.effective_chat.id
        
        if not self.is_owner(chat_id):
            await update.message.reply_text("You don't have permission to use this command. Please contact the bot owner.")
            return
        
        total_keywords = sum(len(g['keywords']) for g in self.groups.values())
        total_processed = sum(len(items) for items in self.processed_items.values())
        enabled_groups = sum(1 for g in self.groups.values() if g['enabled'])
        
        status_msg = f"Bot Status:\n\n"
        status_msg += f"Total groups: {len(self.groups)}\n"
        status_msg += f"Active groups: {enabled_groups}\n"
        status_msg += f"Total keywords: {total_keywords}\n"
        status_msg += f"Total items processed: {total_processed}\n"
        status_msg += f"Check interval: {self.check_interval} seconds\n"
        status_msg += f"Search limit: {self.search_limit} per keyword\n"
        status_msg += f"Time filter: {self.search_time_filter}\n"
        status_msg += f"Reddit client: {'Active' if self.reddit else 'Not initialized'}\n"
        status_msg += f"Slack workspaces: {len(self.slack_workspaces)}\n"
        status_msg += f"Slack clients: {len(self.slack_clients)} active\n"
        status_msg += f"Queued notifications: {len(self.pending_notifications)}\n"
        status_msg += f"Comment stream: {'Running' if self.stream_task and not self.stream_task.done() else 'Stopped'}\n"
        status_msg += f"Notification processor: {'Running' if self.notification_task and not self.notification_task.done() else 'Stopped'}"
        
        await update.message.reply_text(status_msg)
    
    async def export_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Export bot data for manual environment variable backup (Owner only)"""
        chat_id = update.effective_chat.id
        
        if not self.is_owner(chat_id):
            await update.message.reply_text("You don't have permission to use this command. Please contact the bot owner.")
            return
        
        try:
            # Prepare data for export
            groups_data = {}
            for group_id, group_info in self.groups.items():
                groups_data[str(group_id)] = {
                    'name': group_info['name'],
                    'keywords': list(group_info['keywords']),
                    'subreddits': list(group_info.get('subreddits', set())),
                    'subreddit_blacklist': list(group_info.get('subreddit_blacklist', set())),
                    'enabled': group_info['enabled'],
                    'platform': group_info.get('platform', 'telegram'),
                    'channel_id': group_info.get('channel_id', str(group_id)),
                    'workspace_id': group_info.get('workspace_id', '')
                }
            
            processed_data = {}
            for group_id, items in self.processed_items.items():
                processed_data[str(group_id)] = list(items)
            
            data = {
                'groups': groups_data,
                'processed_items': processed_data,
                'last_search_time': self.last_search_time,
                'slack_workspaces': self.slack_workspaces
            }
            
            # Create compact JSON for environment variable
            data_json = json.dumps(data, separators=(',', ':'))
            
            # Save to file
            self.save_data()  # This also creates the export file
            
            # Send instructions and the JSON (in parts if too long)
            instructions = (
                "📦 Bot Data Export\n\n"
                "To preserve your data on Render:\n\n"
                "1. Copy the JSON below\n"
                "2. Go to Render Dashboard → Your Service → Environment\n"
                "3. Add/Update environment variable: BOT_DATA_JSON\n"
                "4. Paste the JSON as the value\n"
                "5. Save and redeploy\n\n"
                "Your data will persist across deployments! ✅\n\n"
                f"Data directory: {self.data_dir}\n"
                f"Export file: {os.path.join(self.data_dir, 'bot_data_env_export.txt')}\n\n"
                "Current data summary:\n"
                f"• Groups: {len(self.groups)}\n"
                f"• Total keywords: {sum(len(g['keywords']) for g in self.groups.values())}\n"
                f"• Slack workspaces: {len(self.slack_workspaces)}\n\n"
                "JSON (copy this):\n"
            )
            
            # Split message if too long (Telegram limit is 4096 characters)
            if len(instructions) + len(data_json) > 4000:
                await update.message.reply_text(instructions)
                # Send JSON in chunks if needed
                chunk_size = 4000
                for i in range(0, len(data_json), chunk_size):
                    chunk = data_json[i:i+chunk_size]
                    await update.message.reply_text(f"`{chunk}`", parse_mode='Markdown')
            else:
                await update.message.reply_text(instructions + f"`{data_json}`", parse_mode='Markdown')
            
            logger.info("Data export requested by owner")
            
        except Exception as e:
            logger.error(f"Error exporting data: {e}")
            await update.message.reply_text(f"Error exporting data: {e}")
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show help message"""
        chat_id = update.effective_chat.id
        
        if self.is_owner(chat_id):
            # Full help for owner
            help_text = """
Reddit Monitor Bot - Multi-Platform (Telegram & Slack)

Owner Commands:

Slack Workspace Management:
/addslack <workspace_id> <bot_token> [name] - Add Slack workspace
  Example: /addslack mywork xoxb-1234-5678-abcd My Workspace
/listslack - List configured Slack workspaces
/removeslack <workspace_id> - Remove a Slack workspace

Group Management:
/addgroup <platform:identifier> [name] - Add Telegram or Slack channel
  Examples:
    /addgroup telegram:-1001234567890 Marketing Team
    /addgroup slack:mywork:C01234ABCDE Sales Channel
/removegroup <group_id> - Remove a group
/listgroups - List all monitored groups
/togglegroup <group_id> - Enable/disable a group

Keyword Management (Two Methods):

Interactive Menu (Recommended):
/group - Opens interactive menu to manage groups
   • Select group
   • Add/Remove keywords via buttons
                   • Add/Remove subreddits (default All if none set)
   • Manage subreddit blacklist to block specific subs
   • List, clear, or toggle group
   • No need to remember group IDs

Direct Commands (Advanced):
/addkeyword <group_id> keyword1, keyword2 - Add keywords directly
/removekeyword <group_id> keyword1, keyword2 - Remove keywords
/listkeywords <group_id> - List keywords for a group
/cleargroup <group_id> - Clear all keywords from a group

Information:
/status - Show bot status
/exportdata - Export bot data for Render deployment backup
/help - Show this help message

Features:
• Searches ALL Reddit posts for keywords
• Searches comments in recent posts
• Real-time comment streaming
• Phrase matching (exact words)
• Multi-group support with separate keywords
• Rate limiting and error recovery

Example - Interactive Menu:
1. /group
2. Click "Marketing Team"
3. Click "Add Keywords"
4. Send: pain killer, mutual fund, crypto news

Example - Direct Command:
/addkeyword -1001234567890 pain killer, mutual fund

Note: Other groups can only receive alerts. All management is done from this control group.
            """
        else:
            # Limited help for non-owner groups
            help_text = """
Reddit to Telegram Monitor Bot

This bot monitors Reddit for specific keywords and sends alerts to this group.

This group is in read-only mode.
Commands can only be used by the bot owner in the control group.

You will receive alerts when keywords match posts or comments on Reddit.

For assistance, please contact the bot owner.
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
        
        # Initialize Slack (optional)
        await self.setup_slack()
        
        # Create Telegram application
        app = Application.builder().token(self.telegram_token).build()
        
        # Add handlers (owner only commands)
        app.add_handler(CommandHandler("addslack", self.addslack))
        app.add_handler(CommandHandler("listslack", self.listslack))
        app.add_handler(CommandHandler("removeslack", self.removeslack))
        app.add_handler(CommandHandler("addgroup", self.addgroup))
        app.add_handler(CommandHandler("removegroup", self.removegroup))
        app.add_handler(CommandHandler("group", self.group))
        app.add_handler(CommandHandler("addkeyword", self.addkeyword))
        app.add_handler(CommandHandler("removekeyword", self.remove_keyword_cmd))
        app.add_handler(CommandHandler("listgroups", self.listgroups))
        app.add_handler(CommandHandler("listkeywords", self.listkeywords))
        app.add_handler(CommandHandler("cleargroup", self.cleargroup))
        app.add_handler(CommandHandler("togglegroup", self.togglegroup))
        app.add_handler(CommandHandler("status", self.status))
        app.add_handler(CommandHandler("exportdata", self.export_data))
        app.add_handler(CommandHandler("help", self.help_command))
        app.add_handler(CommandHandler("start", self.help_command))
        
        # Add callback query handler for group selection
        app.add_handler(CallbackQueryHandler(self.group_selection_callback))
        
        # Add message handler for keyword input (must be last)
        from telegram.ext import MessageHandler, filters
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        
        # Start Telegram bot
        await app.initialize()
        await app.start()
        await app.updater.start_polling()
        
        logger.info("Telegram bot started")
        
        # Start notification processor in background
        self.notification_task = asyncio.create_task(self.notification_processor())
        logger.info("Notification processor started")
        
        # Start comment stream in background
        self.stream_task = asyncio.create_task(self.stream_comments())
        logger.info("Comment stream started")
        
        # Start monitoring loop
        await self.monitoring_loop()

    async def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up resources...")
        
        # Save data before shutdown
        self.save_data()
        
        # Stop background tasks
        self.stop_stream = True
        self.stop_notification_processor = True
        
        # Wait for notification processor to finish
        if self.notification_task:
            self.notification_task.cancel()
            try:
                await self.notification_task
            except asyncio.CancelledError:
                pass
        
        # Wait for stream task to finish
        if self.stream_task:
            self.stream_task.cancel()
            try:
                await self.stream_task
            except asyncio.CancelledError:
                pass
        
        # Close Reddit client
        try:
            if self.reddit:
                await self.reddit.close()
        except Exception as e:
            logger.error(f"Error closing Reddit client: {e}")
        
        # Close Reddit session
        try:
            if self.reddit_session and not self.reddit_session.closed:
                await self.reddit_session.close()
        except Exception as e:
            logger.error(f"Error closing Reddit session: {e}")
        
        # Close Telegram session
        try:
            if self.telegram_session and not self.telegram_session.closed:
                await self.telegram_session.close()
        except Exception as e:
            logger.error(f"Error closing Telegram session: {e}")
        
        logger.info("Cleanup completed")

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
