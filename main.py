import discord
from discord.ext import commands, tasks
import asyncio
import json
import os
from datetime import datetime, timedelta
import pytz
from typing import List, Optional
from flask import Flask
from threading import Thread

# Bot setup
intents = discord.Intents.all()
bot = commands.Bot(command_prefix='!', intents=intents, help_command=None)

# Configuration
GUILD_ID = 1403359962369097739
TRACKING_CHANNEL_ID = 1403422664521023648
MUTE_LOG_CHANNEL_ID = 1410458084874260592
MUTE_ROLE_ID = 1410423854563721287
STAFF_PING_ROLE = 1410422475942264842
HIGHER_STAFF_PING_ROLE = 1410422656112791592
RCACHE_ROLES = [1410422029236047975, 1410422762895577088, 1406326282429403306]
DANGEROUS_LOG_USERS = [1406326282429403306, 1410422762895577088, 1410422029236047975]

# Timezones for display
TIMEZONES = {
    'EST': pytz.timezone('America/New_York'),
    'PST': pytz.timezone('America/Los_Angeles'),
    'GMT': pytz.timezone('GMT'),
    'JST': pytz.timezone('Asia/Tokyo')
}

# Data storage
DATA_FILE = 'bot_data.json'

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f:
            return json.load(f)
    return {
        'users': {},
        'mutes': {},
        'rmute_usage': {},
        'cached_messages': [],
        'rdm_users': [],
        'logs': []
    }

def save_data():
    with open(DATA_FILE, 'w') as f:
        json.dump(bot_data, f, indent=4)

bot_data = load_data()

# Helper functions
def get_user_data(user_id: int):
    user_id_str = str(user_id)
    now = datetime.now(pytz.utc)
    if user_id_str not in bot_data['users']:
        bot_data['users'][user_id_str] = {
            'last_online': None,
            'last_message': None,
            'last_edit': None,
            'total_online_seconds': 0,
            'online_start': None,
            'daily_seconds': 0,
            'weekly_seconds': 0,
            'monthly_seconds': 0,
            'last_reset': {
                'daily': now.isoformat(),
                'weekly': now.isoformat(),
                'monthly': now.isoformat()
            },
            'offline_start': None
        }
    return bot_data['users'][user_id_str]

def format_duration(seconds: int) -> str:
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")
    
    return " ".join(parts)

def parse_duration(duration_str: str) -> int:
    """Parse duration string like '1d', '2h', '30m' into seconds"""
    total_seconds = 0
    current_num = ""
    
    for char in duration_str:
        if char.isdigit():
            current_num += char
        elif char in ['d', 'h', 'm', 's']:
            if current_num:
                num = int(current_num)
                if char == 'd':
                    total_seconds += num * 86400
                elif char == 'h':
                    total_seconds += num * 3600
                elif char == 'm':
                    total_seconds += num * 60
                elif char == 's':
                    total_seconds += num
                current_num = ""
    
    return total_seconds

def format_time_in_timezones(dt: datetime) -> str:
    lines = []
    for name, tz in TIMEZONES.items():
        local_time = dt.astimezone(tz)
        lines.append(f"**{name}:** {local_time.strftime('%Y-%m-%d %I:%M:%S %p')}")
    return "\n".join(lines)

def get_next_reset_times(user_data: dict) -> dict:
    """Calculate when the next resets will occur"""
    now = datetime.now(pytz.utc)
    last_resets = user_data.get('last_reset', {})
    
    daily_last = datetime.fromisoformat(last_resets.get('daily', now.isoformat()))
    weekly_last = datetime.fromisoformat(last_resets.get('weekly', now.isoformat()))
    monthly_last = datetime.fromisoformat(last_resets.get('monthly', now.isoformat()))
    
    daily_next = daily_last + timedelta(days=1)
    weekly_next = weekly_last + timedelta(weeks=1)
    monthly_next = monthly_last + timedelta(days=30)
    
    return {
        'daily': daily_next,
        'weekly': weekly_next,
        'monthly': monthly_next
    }

async def send_dm_safe(user: discord.User, embed: discord.Embed):
    if str(user.id) in bot_data['rdm_users']:
        return
    try:
        await user.send(embed=embed)
    except:
        pass

async def log_action(title: str, description: str = None, color: discord.Color = discord.Color.blue(), fields: list = None, dangerous: bool = False):
    """Central logging function for all bot actions"""
    log_channel = bot.get_channel(MUTE_LOG_CHANNEL_ID)
    if not log_channel:
        return
    
    embed = discord.Embed(
        title=title,
        description=description,
        color=color,
        timestamp=datetime.now(pytz.utc)
    )
    
    if fields:
        for field in fields:
            embed.add_field(name=field['name'], value=field['value'], inline=field.get('inline', False))
    
    await log_channel.send(embed=embed)
    
    # Send to dangerous log users if flagged
    if dangerous:
        for user_id in DANGEROUS_LOG_USERS:
            try:
                user = await bot.fetch_user(user_id)
                await user.send(embed=embed)
            except:
                pass

# Flask keep-alive server for Render
app = Flask('')

@app.route('/')
def home():
    return "✅ Discord Bot is running!"

@app.route('/health')
def health():
    return {"status": "healthy", "bot": str(bot.user) if bot.user else "starting"}

def run_flask():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    t = Thread(target=run_flask)
    t.daemon = True
    t.start()

# Events
@bot.event
async def on_ready():
    print(f'✅ Bot logged in as {bot.user}')
    print(f'📊 Tracking {len(bot_data["users"])} users')
    if not timetrack_loop.is_running():
        timetrack_loop.start()

@bot.event
async def on_message(message):
    if message.author.bot:
        return
    
    # Update last message
    user_data = get_user_data(message.author.id)
    user_data['last_message'] = {
        'content': message.content,
        'timestamp': datetime.now(pytz.utc).isoformat(),
        'channel_id': message.channel.id
    }
    
    # Cache message for deletion tracking (including old messages)
    bot_data['cached_messages'].append({
        'id': message.id,
        'author_id': message.author.id,
        'author_name': str(message.author),
        'content': message.content,
        'attachments': [att.url for att in message.attachments],
        'embeds': [e.to_dict() for e in message.embeds],
        'channel_id': message.channel.id,
        'timestamp': datetime.now(pytz.utc).isoformat(),
        'reference': message.reference.message_id if message.reference else None,
        'created_at': message.created_at.isoformat()
    })
    
    # Keep only last 2000 messages for better coverage
    if len(bot_data['cached_messages']) > 2000:
        bot_data['cached_messages'] = bot_data['cached_messages'][-2000:]
    
    save_data()
    await bot.process_commands(message)

@bot.event
async def on_message_delete(message):
    if message.author.bot:
        return
    
    # Calculate message age
    message_age = datetime.now(pytz.utc) - message.created_at.replace(tzinfo=pytz.utc)
    
    embed = discord.Embed(
        title="🗑️ Message Deleted",
        color=discord.Color.red(),
        timestamp=datetime.now(pytz.utc)
    )
    embed.add_field(name="👤 Author", value=f"{message.author.mention} ({message.author})", inline=False)
    embed.add_field(name="📍 Channel", value=message.channel.mention, inline=True)
    embed.add_field(name="⏰ Message Age", value=format_duration(int(message_age.total_seconds())), inline=True)
    
    if message.content:
        content = message.content[:1024]
        embed.add_field(name="📝 Content", value=content, inline=False)
    
    if message.attachments:
        attachments = "\n".join([att.url for att in message.attachments][:5])
        embed.add_field(name="📎 Attachments", value=attachments, inline=False)
    
    if message.embeds:
        embed.add_field(name="📊 Embeds", value=f"{len(message.embeds)} embed(s)", inline=False)
    
    embed.add_field(name="🕐 Deleted At", value=format_time_in_timezones(datetime.now(pytz.utc)), inline=False)
    
    await log_action(
        title=embed.title,
        description=embed.description,
        color=embed.color,
        fields=[{"name": f.name, "value": f.value, "inline": f.inline} for f in embed.fields]
    )

@bot.event
async def on_message_edit(before, after):
    if before.author.bot or before.content == after.content:
        return
    
    user_data = get_user_data(before.author.id)
    user_data['last_edit'] = datetime.now(pytz.utc).isoformat()
    save_data()
    
    # Log message edit
    await log_action(
        title="✏️ Message Edited",
        color=discord.Color.orange(),
        fields=[
            {"name": "👤 Author", "value": f"{before.author.mention} ({before.author})", "inline": False},
            {"name": "📍 Channel", "value": before.channel.mention, "inline": True},
            {"name": "📝 Before", "value": before.content[:1024] if before.content else "No content", "inline": False},
            {"name": "📝 After", "value": after.content[:1024] if after.content else "No content", "inline": False},
            {"name": "🔗 Jump", "value": f"[View Message]({after.jump_url})", "inline": False}
        ]
    )

@bot.event
async def on_member_join(member):
    await log_action(
        title="👋 Member Joined",
        color=discord.Color.green(),
        fields=[
            {"name": "👤 Member", "value": f"{member.mention} ({member})", "inline": False},
            {"name": "🆔 User ID", "value": str(member.id), "inline": True},
            {"name": "📅 Account Created", "value": member.created_at.strftime('%Y-%m-%d'), "inline": True},
            {"name": "👥 Member Count", "value": str(member.guild.member_count), "inline": True}
        ]
    )

@bot.event
async def on_member_remove(member):
    await log_action(
        title="👋 Member Left",
        color=discord.Color.red(),
        fields=[
            {"name": "👤 Member", "value": f"{member.mention} ({member})", "inline": False},
            {"name": "🆔 User ID", "value": str(member.id), "inline": True},
            {"name": "📅 Joined Server", "value": member.joined_at.strftime('%Y-%m-%d') if member.joined_at else "Unknown", "inline": True},
            {"name": "👥 Member Count", "value": str(member.guild.member_count), "inline": True}
        ]
    )

@bot.event
async def on_member_update(before, after):
    # Nickname change
    if before.nick != after.nick:
        await log_action(
            title="✏️ Nickname Changed",
            color=discord.Color.blue(),
            fields=[
                {"name": "👤 Member", "value": f"{after.mention} ({after})", "inline": False},
                {"name": "📝 Old Nickname", "value": before.nick or "None", "inline": True},
                {"name": "📝 New Nickname", "value": after.nick or "None", "inline": True}
            ]
        )
    
    # Role changes
    if before.roles != after.roles:
        added_roles = [role for role in after.roles if role not in before.roles]
        removed_roles = [role for role in before.roles if role not in after.roles]
        
        fields = [{"name": "👤 Member", "value": f"{after.mention} ({after})", "inline": False}]
        
        if added_roles:
            fields.append({"name": "➕ Roles Added", "value": ", ".join([r.mention for r in added_roles]), "inline": False})
        
        if removed_roles:
            fields.append({"name": "➖ Roles Removed", "value": ", ".join([r.mention for r in removed_roles]), "inline": False})
        
        await log_action(
            title="🎭 Member Roles Updated",
            color=discord.Color.purple(),
            fields=fields
        )

@bot.event
async def on_member_ban(guild, user):
    await log_action(
        title="🔨 Member Banned",
        color=discord.Color.dark_red(),
        fields=[
            {"name": "👤 User", "value": f"{user.mention} ({user})", "inline": False},
            {"name": "🆔 User ID", "value": str(user.id), "inline": True}
        ],
        dangerous=True
    )

@bot.event
async def on_member_unban(guild, user):
    await log_action(
        title="🔓 Member Unbanned",
        color=discord.Color.green(),
        fields=[
            {"name": "👤 User", "value": f"{user.mention} ({user})", "inline": False},
            {"name": "🆔 User ID", "value": str(user.id), "inline": True}
        ]
    )

# Timetrack loop with fixed tracking
@tasks.loop(seconds=60)
async def timetrack_loop():
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return
    
    tracking_channel = bot.get_channel(MUTE_LOG_CHANNEL_ID)
    now = datetime.now(pytz.utc)
    
    for member in guild.members:
        if member.bot:
            continue
        
        # Check if user has tracking roles
        has_tracking_role = any(role.id in RCACHE_ROLES for role in member.roles)
        if not has_tracking_role:
            continue
        
        user_data = get_user_data(member.id)
        
        # Check if user sent message recently (within 53 seconds)
        if user_data.get('last_message'):
            last_msg_time = datetime.fromisoformat(user_data['last_message']['timestamp'])
            time_since_msg = (now - last_msg_time).total_seconds()
            
            if time_since_msg <= 53:
                # User is online
                if user_data.get('online_start'):
                    # Already online, continue counting - ADD 60 seconds each loop
                    user_data['total_online_seconds'] += 60
                    user_data['daily_seconds'] += 60
                    user_data['weekly_seconds'] += 60
                    user_data['monthly_seconds'] += 60
                else:
                    # Just came online
                    user_data['online_start'] = now.isoformat()
                    user_data['offline_start'] = None
                    
                    if tracking_channel:
                        embed = discord.Embed(
                            title="🟢 User Online",
                            color=discord.Color.green(),
                            timestamp=now
                        )
                        embed.set_author(name=str(member), icon_url=member.display_avatar.url)
                        embed.add_field(name="User", value=member.mention, inline=False)
                        if user_data.get('last_message'):
                            embed.add_field(name="Last Message", value=user_data['last_message'].get('content', 'N/A')[:100], inline=False)
                        await tracking_channel.send(embed=embed)
            else:
                # User is offline
                if user_data.get('online_start'):
                    # Just went offline
                    user_data['online_start'] = None
                    user_data['offline_start'] = now.isoformat()
                    
                    if tracking_channel:
                        embed = discord.Embed(
                            title="🔴 User Offline",
                            color=discord.Color.orange(),
                            timestamp=now
                        )
                        embed.set_author(name=str(member), icon_url=member.display_avatar.url)
                        embed.add_field(name="User", value=member.mention, inline=False)
                        await tracking_channel.send(embed=embed)
    
    # Reset daily/weekly/monthly counters
    for user_id, user_data in bot_data['users'].items():
        last_resets = user_data.get('last_reset', {})
        
        # Daily reset
        if last_resets.get('daily'):
            last_daily = datetime.fromisoformat(last_resets['daily'])
            if (now - last_daily).days >= 1:
                user_data['daily_seconds'] = 0
                user_data['last_reset']['daily'] = now.isoformat()
        
        # Weekly reset
        if last_resets.get('weekly'):
            last_weekly = datetime.fromisoformat(last_resets['weekly'])
            if (now - last_weekly).days >= 7:
                user_data['weekly_seconds'] = 0
                user_data['last_reset']['weekly'] = now.isoformat()
        
        # Monthly reset
        if last_resets.get('monthly'):
            last_monthly = datetime.fromisoformat(last_resets['monthly'])
            if (now - last_monthly).days >= 30:
                user_data['monthly_seconds'] = 0
                user_data['last_reset']['monthly'] = now.isoformat()
    
    save_data()

# Commands
@bot.command(name='rhelp')
async def rhelp(ctx):
    embed = discord.Embed(
        title="📚 Bot Commands Help",
        description="Complete list of available commands",
        color=discord.Color.blue()
    )
    
    embed.add_field(
        name="!timetrack [user]",
        value="Shows detailed online/offline tracking stats for a user",
        inline=False
    )
    
    embed.add_field(
        name="!rmute [users] [duration] [reason]",
        value="Mute multiple users. Duration: 1d, 2h, 30m, etc.",
        inline=False
    )
    
    embed.add_field(
        name="!runmute [user] [reason]",
        value="Unmute a user and log the action",
        inline=False
    )
    
    embed.add_field(
        name="!rmlb",
        value="Shows leaderboard of who used !rmute the most",
        inline=False
    )
    
    embed.add_field(
        name="!rcache",
        value="Shows recently deleted images/files (requires specific roles)",
        inline=False
    )
    
    embed.add_field(
        name="!tlb",
        value="Timetrack leaderboard (users with tracked roles)",
        inline=False
    )
    
    embed.add_field(
        name="!tdm",
        value="Timetrack leaderboard (users without tracked roles)",
        inline=False
    )
    
    embed.add_field(
        name="!sping / !hsping",
        value="Ping staff roles with optional reply context",
        inline=False
    )
    
    embed.add_field(
        name="!rdm",
        value="Opt out of bot DM notifications",
        inline=False
    )
    
    embed.set_footer(text="Use these commands responsibly!")
    await ctx.send(embed=embed)

@bot.command(name='timetrack')
async def timetrack(ctx, member: discord.Member = None):
    if member is None:
        member = ctx.author
    
    user_data = get_user_data(member.id)
    now = datetime.now(pytz.utc)
    
    embed = discord.Embed(
        title=f"⏰ Timetrack for {member.display_name}",
        color=discord.Color.blue(),
        timestamp=now
    )
    embed.set_thumbnail(url=member.display_avatar.url)
    
    # Online/Offline status
    if user_data.get('online_start'):
        online_duration = int((now - datetime.fromisoformat(user_data['online_start'])).total_seconds())
        embed.add_field(name="🟢 Status", value=f"Online for {format_duration(online_duration)}", inline=False)
    elif user_data.get('offline_start'):
        offline_duration = int((now - datetime.fromisoformat(user_data['offline_start'])).total_seconds())
        embed.add_field(name="🔴 Status", value=f"Offline for {format_duration(offline_duration)}", inline=False)
    else:
        embed.add_field(name="Status", value="Unknown", inline=False)
    
    # Last message
    if user_data.get('last_message'):
        last_msg = user_data['last_message']
        msg_time = datetime.fromisoformat(last_msg['timestamp'])
        embed.add_field(
            name="💬 Last Message",
            value=f"{last_msg.get('content', 'N/A')[:100]}\n\n{format_time_in_timezones(msg_time)}",
            inline=False
        )
    
    # Time stats - NOW SHOWING PROPERLY
    total = user_data.get('total_online_seconds', 0)
    daily = user_data.get('daily_seconds', 0)
    weekly = user_data.get('weekly_seconds', 0)
    monthly = user_data.get('monthly_seconds', 0)
    
    embed.add_field(name="📊 Total Time Online (All Time)", value=format_duration(total), inline=False)
    embed.add_field(name="📅 Today", value=format_duration(daily), inline=True)
    embed.add_field(name="📆 This Week", value=format_duration(weekly), inline=True)
    embed.add_field(name="📈 This Month", value=format_duration(monthly), inline=True)
    
    # Next reset times
    next_resets = get_next_reset_times(user_data)
    reset_info = []
    for period, reset_time in next_resets.items():
        time_until = reset_time - now
        reset_info.append(f"**{period.capitalize()}:** {format_duration(int(time_until.total_seconds()))}")
    
    embed.add_field(name="🔄 Next Resets In", value="\n".join(reset_info), inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name='rmute')
async def rmute(ctx, members: commands.Greedy[discord.Member], duration: str, *, reason: str):
    if not members:
        await ctx.send("❌ Please specify at least one user to mute.")
        return
    
    mute_role = ctx.guild.get_role(MUTE_ROLE_ID)
    if not mute_role:
        await ctx.send("❌ Mute role not found!")
        return
    
    duration_seconds = parse_duration(duration)
    if duration_seconds == 0:
        await ctx.send("❌ Invalid duration format. Use format like: 1d, 2h, 30m")
        return
    
    tracking_channel = bot.get_channel(MUTE_LOG_CHANNEL_ID)
    unmute_time = datetime.now(pytz.utc) + timedelta(seconds=duration_seconds)
    
    # Track rmute usage
    mod_id_str = str(ctx.author.id)
    bot_data['rmute_usage'][mod_id_str] = bot_data['rmute_usage'].get(mod_id_str, 0) + len(members)
    
    for member in members:
        # Apply mute role
        await member.add_roles(mute_role, reason=reason)
        
        # Timeout using Discord API
        try:
            await member.timeout(timedelta(seconds=duration_seconds), reason=reason)
        except:
            pass
        
        # Store mute data
        bot_data['mutes'][str(member.id)] = {
            'moderator_id': ctx.author.id,
            'reason': reason,
            'duration': duration_seconds,
            'start_time': datetime.now(pytz.utc).isoformat(),
            'unmute_time': unmute_time.isoformat()
        }
        
        # Send fancy DM to user
        dm_embed = discord.Embed(
            title="🔇 You Have Been Muted",
            description=f"You have been muted in **{ctx.guild.name}**",
            color=discord.Color.red(),
            timestamp=datetime.now(pytz.utc)
        )
        dm_embed.add_field(name="⚠️ Reason", value=reason, inline=False)
        dm_embed.add_field(name="⏱️ Duration", value=format_duration(duration_seconds), inline=True)
        dm_embed.add_field(name="👮 Moderator", value=ctx.author.mention, inline=True)
        dm_embed.add_field(name="🕐 Unmute Time", value=format_time_in_timezones(unmute_time), inline=False)
        dm_embed.set_footer(text="Please follow the server rules.")
        
        await send_dm_safe(member, dm_embed)
        
        # Log in tracking channel
        if tracking_channel:
            log_embed = discord.Embed(
                title="🔨 User Muted",
                color=discord.Color.red(),
                timestamp=datetime.now(pytz.utc)
            )
            log_embed.set_thumbnail(url=member.display_avatar.url)
            log_embed.add_field(name="👤 User", value=f"{member.mention} ({member})", inline=False)
            log_embed.add_field(name="👮 Moderator", value=ctx.author.mention, inline=False)
            log_embed.add_field(name="⚠️ Reason", value=reason, inline=False)
            log_embed.add_field(name="⏱️ Duration", value=format_duration(duration_seconds), inline=True)
            log_embed.add_field(name="🕐 Unmute Time", value=format_time_in_timezones(unmute_time), inline=False)
            
            await tracking_channel.send(embed=log_embed)
        
        # Schedule auto-unmute
        asyncio.create_task(auto_unmute(member, duration_seconds, reason, ctx.author))
    
    save_data()
    await ctx.send(f"✅ Muted {len(members)} user(s) for {format_duration(duration_seconds)}")

async def auto_unmute(member: discord.Member, duration: int, original_reason: str, moderator: discord.Member):
    await asyncio.sleep(duration)
    
    mute_role = member.guild.get_role(MUTE_ROLE_ID)
    if mute_role in member.roles:
        await member.remove_roles(mute_role, reason="Auto-unmute")
        
        try:
            await member.timeout(None, reason="Auto-unmute")
        except:
            pass
        
        # Remove from mutes data
        if str(member.id) in bot_data['mutes']:
            del bot_data['mutes'][str(member.id)]
            save_data()
        
        # Log auto-unmute
        tracking_channel = member.guild.get_channel(MUTE_LOG_CHANNEL_ID)
        if tracking_channel:
            embed = discord.Embed(
                title="🔓 Auto-Unmute",
                description=f"{member.mention} has been automatically unmuted",
                color=discord.Color.green(),
                timestamp=datetime.now(pytz.utc)
            )
            embed.add_field(name="Original Reason", value=original_reason, inline=False)
            embed.add_field(name="Muted By", value=moderator.mention, inline=True)
            embed.add_field(name="Duration", value=format_duration(duration), inline=True)
            
            await tracking_channel.send(embed=embed)

@bot.command(name='runmute')
async def runmute(ctx, member: discord.Member, *, reason: str):
    mute_role = ctx.guild.get_role(MUTE_ROLE_ID)
    if not mute_role or mute_role not in member.roles:
        await ctx.send("❌ This user is not muted.")
        return
    
    # Get original mute data
    mute_data = bot_data['mutes'].get(str(member.id), {})
    
    # Remove mute role
    await member.remove_roles(mute_role, reason=reason)
    
    # Remove timeout
    try:
        await member.timeout(None, reason=reason)
    except:
        pass
    
    # Calculate duration
    if mute_data.get('start_time'):
        start = datetime.fromisoformat(mute_data['start_time'])
        duration = int((datetime.now(pytz.utc) - start).total_seconds())
    else:
        duration = 0
    
    # Log in tracking channel
    tracking_channel = bot.get_channel(MUTE_LOG_CHANNEL_ID)
    if tracking_channel:
        embed = discord.Embed(
            title="🔓 User Unmuted",
            color=discord.Color.green(),
            timestamp=datetime.now(pytz.utc)
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="👤 Unmuted User", value=f"{member.mention} ({member})", inline=False)
        embed.add_field(name="👮 Unmuted By", value=ctx.author.mention, inline=False)
        embed.add_field(name="⏱️ Mute Duration", value=format_duration(duration), inline=False)
        embed.add_field(name="⚠️ Unmute Reason", value=reason, inline=False)
        
        if mute_data:
            original_mod = ctx.guild.get_member(mute_data.get('moderator_id', 0))
            if original_mod:
                embed.add_field(name="Originally Muted By", value=original_mod.mention, inline=True)
            if mute_data.get('reason'):
                embed.add_field(name="Original Reason", value=mute_data['reason'], inline=False)
        
        await tracking_channel.send(embed=embed)
    
    # Send DM
    dm_embed = discord.Embed(
        title="🔓 You Have Been Unmuted",
        description=f"You have been unmuted in **{ctx.guild.name}**",
        color=discord.Color.green(),
        timestamp=datetime.now(pytz.utc)
    )
    dm_embed.add_field(name="Reason", value=reason, inline=False)
    dm_embed.add_field(name="Unmuted By", value=ctx.author.mention, inline=False)
    
    await send_dm_safe(member, dm_embed)
    
    # Remove from mutes data
    if str(member.id) in bot_data['mutes']:
        del bot_data['mutes'][str(member.id)]
        save_data()
    
    await ctx.send(f"✅ {member.mention} has been unmuted.")

@bot.command(name='rmlb')
async def rmlb(ctx):
    if not bot_data['rmute_usage']:
        await ctx.send("❌ No mute usage data available.")
        return
    
    sorted_usage = sorted(bot_data['rmute_usage'].items(), key=lambda x: x[1], reverse=True)[:10]
    
    embed = discord.Embed(
        title="🏆 RMute Usage Leaderboard",
        description="Top 10 moderators by mute count",
        color=discord.Color.gold(),
        timestamp=datetime.now(pytz.utc)
    )
    
    for i, (user_id, count) in enumerate(sorted_usage, 1):
        user = await bot.fetch_user(int(user_id))
        embed.add_field(
            name=f"#{i} {user.name}",
            value=f"🔨 {count} mutes",
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command(name='rcache')
async def rcache(ctx):
    # Check if user has required roles
    has_role = any(role.id in RCACHE_ROLES for role in ctx.author.roles)
    if not has_role:
        await ctx.send("❌ You don't have permission to use this command.")
        return
    
    if not bot_data['cached_messages']:
        await ctx.send("❌ No cached messages available.")
        return
    
    recent_messages = bot_data['cached_messages'][-10:]
    
    embed = discord.Embed(
        title="🗂️ Recent Cached Messages",
        color=discord.Color.blue(),
        timestamp=datetime.now(pytz.utc)
    )
    
    for msg in recent_messages:
        try:
            author = await bot.fetch_user(msg['author_id'])
            field_value = f"**Author:** {author.mention}\n"
        except:
            field_value = f"**Author:** {msg.get('author_name', 'Unknown')}\n"
        
        if msg.get('content'):
            field_value += f"**Content:** {msg['content'][:100]}\n"
        
        if msg.get('attachments'):
            field_value += f"**Attachments:** {', '.join(msg['attachments'][:3])}\n"
        
        if msg.get('reference'):
            field_value += f"**Reply to:** Message ID {msg['reference']}\n"
        
        # Show message age
        if msg.get('created_at'):
            created = datetime.fromisoformat(msg['created_at'])
            age = datetime.now(pytz.utc) - created.replace(tzinfo=pytz.utc)
            field_value += f"**Age:** {format_duration(int(age.total_seconds()))}\n"
        
        embed.add_field(name=f"Message {msg['id']}", value=field_value[:1024], inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name='tlb')
async def tlb(ctx):
    guild = ctx.guild
    tracked_users = []
    
    for user_id, user_data in bot_data['users'].items():
        member = guild.get_member(int(user_id))
        if member and any(role.id in RCACHE_ROLES for role in member.roles):
            daily_avg = user_data.get('daily_seconds', 0)
            tracked_users.append((member, daily_avg))
    
    tracked_users.sort(key=lambda x: x[1], reverse=True)
    tracked_users = tracked_users[:10]
    
    embed = discord.Embed(
        title="🏆 Timetrack Leaderboard (Tracked Roles)",
        description="Top 10 users by daily online time",
        color=discord.Color.blue(),
        timestamp=datetime.now(pytz.utc)
    )
    
    for i, (member, seconds) in enumerate(tracked_users, 1):
        embed.add_field(
            name=f"#{i} {member.display_name}",
            value=f"⏰ {format_duration(seconds)} today",
            inline=False
        )
    
    if not tracked_users:
        embed.description = "No tracked users found."
    
    await ctx.send(embed=embed)

@bot.command(name='tdm')
async def tdm(ctx):
    guild = ctx.guild
    untracked_users = []
    
    for user_id, user_data in bot_data['users'].items():
        member = guild.get_member(int(user_id))
        if member and not any(role.id in RCACHE_ROLES for role in member.roles):
            daily_avg = user_data.get('daily_seconds', 0)
            untracked_users.append((member, daily_avg))
    
    untracked_users.sort(key=lambda x: x[1], reverse=True)
    untracked_users = untracked_users[:10]
    
    embed = discord.Embed(
        title="🏆 Timetrack Leaderboard (Non-Tracked Roles)",
        description="Top 10 users by daily online time",
        color=discord.Color.purple(),
        timestamp=datetime.now(pytz.utc)
    )
    
    for i, (member, seconds) in enumerate(untracked_users, 1):
        embed.add_field(
            name=f"#{i} {member.display_name}",
            value=f"⏰ {format_duration(seconds)} today",
            inline=False
        )
    
    if not untracked_users:
        embed.description = "No untracked users found."
    
    await ctx.send(embed=embed)

@bot.command(name='sping')
async def sping(ctx):
    await ctx.message.delete()
    
    staff_role = ctx.guild.get_role(STAFF_PING_ROLE)
    if not staff_role:
        await ctx.send("❌ Staff ping role not found!")
        return
    
    # Get channels to log to
    log_channels = [
        bot.get_channel(TRACKING_CHANNEL_ID),
        bot.get_channel(MUTE_LOG_CHANNEL_ID)
    ]
    
    # Check if this is a reply
    reply_info = None
    if ctx.message.reference:
        try:
            replied_message = await ctx.channel.fetch_message(ctx.message.reference.message_id)
            reply_info = {
                'author': replied_message.author,
                'content': replied_message.content[:500],
                'jump_url': replied_message.jump_url
            }
        except:
            pass
    
    # Send the ping
    ping_message = await ctx.send(f"{staff_role.mention}")
    
    # Log the ping
    for log_channel in log_channels:
        if log_channel:
            embed = discord.Embed(
                title="📢 Staff Ping",
                color=discord.Color.blue(),
                timestamp=datetime.now(pytz.utc)
            )
            embed.add_field(name="Pinged By", value=ctx.author.mention, inline=False)
            embed.add_field(name="Channel", value=ctx.channel.mention, inline=False)
            
            if reply_info:
                embed.add_field(name="Reply To", value=reply_info['author'].mention, inline=False)
                embed.add_field(name="Original Message", value=reply_info['content'], inline=False)
                embed.add_field(name="Jump to Message", value=f"[Click Here]({reply_info['jump_url']})", inline=False)
            
            await log_channel.send(embed=embed)

@bot.command(name='hsping')
async def hsping(ctx):
    await ctx.message.delete()
    
    higher_staff_role = ctx.guild.get_role(HIGHER_STAFF_PING_ROLE)
    if not higher_staff_role:
        await ctx.send("❌ Higher staff ping role not found!")
        return
    
    # Get channels to log to
    log_channels = [
        bot.get_channel(TRACKING_CHANNEL_ID),
        bot.get_channel(MUTE_LOG_CHANNEL_ID)
    ]
    
    # Check if this is a reply
    reply_info = None
    if ctx.message.reference:
        try:
            replied_message = await ctx.channel.fetch_message(ctx.message.reference.message_id)
            reply_info = {
                'author': replied_message.author,
                'content': replied_message.content[:500],
                'jump_url': replied_message.jump_url
            }
        except:
            pass
    
    # Send the ping
    ping_message = await ctx.send(f"{higher_staff_role.mention}")
    
    # Log the ping
    for log_channel in log_channels:
        if log_channel:
            embed = discord.Embed(
                title="🚨 Higher Staff Ping",
                color=discord.Color.red(),
                timestamp=datetime.now(pytz.utc)
            )
            embed.add_field(name="Pinged By", value=ctx.author.mention, inline=False)
            embed.add_field(name="Channel", value=ctx.channel.mention, inline=False)
            
            if reply_info:
                embed.add_field(name="Reply To", value=reply_info['author'].mention, inline=False)
                embed.add_field(name="Original Message", value=reply_info['content'], inline=False)
                embed.add_field(name="Jump to Message", value=f"[Click Here]({reply_info['jump_url']})", inline=False)
            
            await log_channel.send(embed=embed)

@bot.command(name='rdm')
async def rdm(ctx):
    user_id_str = str(ctx.author.id)
    
    if user_id_str in bot_data['rdm_users']:
        bot_data['rdm_users'].remove(user_id_str)
        save_data()
        await ctx.send("✅ You will now receive DM notifications from the bot.")
    else:
        bot_data['rdm_users'].append(user_id_str)
        save_data()
        await ctx.send("✅ You have opted out of DM notifications from the bot.")

# Additional event handlers for logging
@bot.event
async def on_guild_channel_create(channel):
    await log_action(
        title="📝 Channel Created",
        color=discord.Color.green(),
        fields=[
            {"name": "Channel", "value": channel.mention, "inline": False},
            {"name": "Channel Type", "value": str(channel.type), "inline": True},
            {"name": "Channel ID", "value": str(channel.id), "inline": True}
        ]
    )

@bot.event
async def on_guild_channel_delete(channel):
    await log_action(
        title="🗑️ Channel Deleted",
        color=discord.Color.red(),
        fields=[
            {"name": "Channel Name", "value": channel.name, "inline": False},
            {"name": "Channel Type", "value": str(channel.type), "inline": True},
            {"name": "Channel ID", "value": str(channel.id), "inline": True}
        ],
        dangerous=True
    )

@bot.event
async def on_guild_channel_update(before, after):
    changes = []
    
    if before.name != after.name:
        changes.append(f"**Name:** {before.name} → {after.name}")
    
    if hasattr(before, 'topic') and hasattr(after, 'topic') and before.topic != after.topic:
        changes.append(f"**Topic Changed**")
    
    if before.category != after.category:
        before_cat = before.category.name if before.category else "None"
        after_cat = after.category.name if after.category else "None"
        changes.append(f"**Category:** {before_cat} → {after_cat}")
    
    # Check permission overwrites
    if before.overwrites != after.overwrites:
        changes.append("**Permissions Modified**")
    
    if changes:
        await log_action(
            title="✏️ Channel Updated",
            color=discord.Color.orange(),
            fields=[
                {"name": "Channel", "value": after.mention, "inline": False},
                {"name": "Changes", "value": "\n".join(changes), "inline": False}
            ]
        )

@bot.event
async def on_guild_role_create(role):
    await log_action(
        title="🎭 Role Created",
        color=discord.Color.green(),
        fields=[
            {"name": "Role", "value": role.mention, "inline": False},
            {"name": "Role ID", "value": str(role.id), "inline": True},
            {"name": "Color", "value": str(role.color), "inline": True}
        ]
    )

@bot.event
async def on_guild_role_delete(role):
    await log_action(
        title="🗑️ Role Deleted",
        color=discord.Color.red(),
        fields=[
            {"name": "Role Name", "value": role.name, "inline": False},
            {"name": "Role ID", "value": str(role.id), "inline": True}
        ],
        dangerous=True
    )

@bot.event
async def on_guild_role_update(before, after):
    changes = []
    
    if before.name != after.name:
        changes.append(f"**Name:** {before.name} → {after.name}")
    
    if before.color != after.color:
        changes.append(f"**Color:** {before.color} → {after.color}")
    
    if before.permissions != after.permissions:
        changes.append("**Permissions Modified**")
    
    if before.hoist != after.hoist:
        changes.append(f"**Display Separately:** {before.hoist} → {after.hoist}")
    
    if before.mentionable != after.mentionable:
        changes.append(f"**Mentionable:** {before.mentionable} → {after.mentionable}")
    
    if changes:
        await log_action(
            title="✏️ Role Updated",
            color=discord.Color.orange(),
            fields=[
                {"name": "Role", "value": after.mention, "inline": False},
                {"name": "Changes", "value": "\n".join(changes), "inline": False}
            ]
        )

@bot.event
async def on_bulk_message_delete(messages):
    message_info = []
    for msg in list(messages)[:10]:
        if not msg.author.bot:
            message_info.append(f"**{msg.author}**: {msg.content[:100]}")
    
    await log_action(
        title="🗑️ Bulk Message Delete (Purge)",
        description=f"{len(messages)} messages were deleted",
        color=discord.Color.red(),
        fields=[
            {"name": "Channel", "value": messages[0].channel.mention if messages else "Unknown", "inline": False},
            {"name": "Sample Messages", "value": "\n".join(message_info) if message_info else "No content", "inline": False}
        ],
        dangerous=True
    )

@bot.event
async def on_guild_update(before, after):
    changes = []
    
    if before.name != after.name:
        changes.append(f"**Name:** {before.name} → {after.name}")
    
    if before.owner != after.owner:
        changes.append(f"**Owner:** {before.owner.mention} → {after.owner.mention}")
    
    if before.verification_level != after.verification_level:
        changes.append(f"**Verification Level:** {before.verification_level} → {after.verification_level}")
    
    if changes:
        await log_action(
            title="⚙️ Server Settings Updated",
            color=discord.Color.blue(),
            fields=[{"name": "Changes", "value": "\n".join(changes), "inline": False}],
            dangerous=True
        )

@bot.event
async def on_webhooks_update(channel):
    await log_action(
        title="🔗 Webhook Updated",
        color=discord.Color.orange(),
        fields=[
            {"name": "Channel", "value": channel.mention, "inline": False}
        ]
    )

# Run the bot
if __name__ == "__main__":
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        print("❌ Error: DISCORD_TOKEN environment variable not set!")
        exit(1)
    
    keep_alive()  # Start Flask server
    bot.run(TOKEN)
