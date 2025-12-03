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
import io

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
MOD_ROLES = [1410422029236047975, 1410422762895577088]

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
        'logs': [],
        'mute_history': {},
        'user_mute_history': {}
    }

def save_data():
    with open(DATA_FILE, 'w') as f:
        json.dump(bot_data, f, indent=4)

bot_data = load_data()

# Save data every 5 minutes to prevent data loss
@tasks.loop(minutes=5)
async def auto_save():
    save_data()
    print("✅ Auto-saved bot data")

# Helper functions
def has_mod_role(member):
    """Check if member has moderator role"""
    return any(role.id in MOD_ROLES for role in member.roles)

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
    
    if dangerous:
        for user_id in DANGEROUS_LOG_USERS:
            try:
                user = await bot.fetch_user(user_id)
                await user.send(embed=embed)
            except:
                pass

# Flask keep-alive
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
    if not auto_save.is_running():
        auto_save.start()

@bot.event
async def on_message(message):
    if message.author.bot:
        return
    
    user_data = get_user_data(message.author.id)
    user_data['last_message'] = {
        'content': message.content,
        'timestamp': datetime.now(pytz.utc).isoformat(),
        'channel_id': message.channel.id
    }
    
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
    
    if len(bot_data['cached_messages']) > 2000:
        bot_data['cached_messages'] = bot_data['cached_messages'][-2000:]
    
    save_data()
    await bot.process_commands(message)

@bot.event
async def on_message_delete(message):
    if message.author.bot:
        return
    
    message_age = datetime.now(pytz.utc) - message.created_at.replace(tzinfo=pytz.utc)
    
    deleter = None
    try:
        guild = message.guild
        async for entry in guild.audit_logs(limit=5, action=discord.AuditLogAction.message_delete):
            if entry.target.id == message.author.id:
                if (datetime.now(pytz.utc) - entry.created_at.replace(tzinfo=pytz.utc)).total_seconds() < 5:
                    deleter = entry.user
                    break
    except:
        pass
    
    fields = [
        {"name": "👤 Author", "value": f"{message.author.mention} ({message.author})", "inline": False},
        {"name": "📍 Channel", "value": message.channel.mention, "inline": True},
        {"name": "⏰ Message Age", "value": format_duration(int(message_age.total_seconds())), "inline": True}
    ]
    
    if deleter:
        fields.append({"name": "🗑️ Deleted By", "value": f"{deleter.mention} ({deleter})", "inline": False})
    
    if message.content:
        fields.append({"name": "📝 Content", "value": message.content[:1024], "inline": False})
    
    if message.attachments:
        attachments = "\n".join([att.url for att in message.attachments][:5])
        fields.append({"name": "📎 Attachments", "value": attachments, "inline": False})
    
    if message.embeds:
        fields.append({"name": "📊 Embeds", "value": f"{len(message.embeds)} embed(s)", "inline": False})
    
    await log_action(
        title="🗑️ Message Deleted",
        color=discord.Color.red(),
        fields=fields
    )

@bot.event
async def on_message_edit(before, after):
    if before.author.bot or before.content == after.content:
        return
    
    user_data = get_user_data(before.author.id)
    user_data['last_edit'] = datetime.now(pytz.utc).isoformat()
    save_data()
    
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
    executor = None
    try:
        async for entry in after.guild.audit_logs(limit=1):
            if (datetime.now(pytz.utc) - entry.created_at.replace(tzinfo=pytz.utc)).total_seconds() < 3:
                executor = entry.user
                break
    except:
        pass
    
    if before.nick != after.nick:
        fields = [
            {"name": "👤 Member", "value": f"{after.mention} ({after})", "inline": False},
            {"name": "📝 Old Nickname", "value": before.nick or "None", "inline": True},
            {"name": "📝 New Nickname", "value": after.nick or "None", "inline": True}
        ]
        if executor:
            fields.append({"name": "✏️ Changed By", "value": f"{executor.mention} ({executor})", "inline": False})
        
        await log_action(
            title="✏️ Nickname Changed",
            color=discord.Color.blue(),
            fields=fields
        )
    
    if before.roles != after.roles:
        added_roles = [role for role in after.roles if role not in before.roles]
        removed_roles = [role for role in before.roles if role not in after.roles]
        
        fields = [{"name": "👤 Member", "value": f"{after.mention} ({after})", "inline": False}]
        
        if added_roles:
            fields.append({"name": "➕ Roles Added", "value": ", ".join([r.mention for r in added_roles]), "inline": False})
        
        if removed_roles:
            fields.append({"name": "➖ Roles Removed", "value": ", ".join([r.mention for r in removed_roles]), "inline": False})
        
        if executor:
            fields.append({"name": "👮 Modified By", "value": f"{executor.mention} ({executor})", "inline": False})
        
        await log_action(
            title="🎭 Member Roles Updated",
            color=discord.Color.purple(),
            fields=fields
        )
    
    if before.timed_out_until != after.timed_out_until:
        if after.timed_out_until and after.timed_out_until > datetime.now(pytz.utc):
            duration = (after.timed_out_until.replace(tzinfo=pytz.utc) - datetime.now(pytz.utc)).total_seconds()
            fields = [
                {"name": "👤 User", "value": f"{after.mention} ({after})", "inline": False},
                {"name": "⏱️ Duration", "value": format_duration(int(duration)), "inline": True},
                {"name": "🕐 Unmute Time", "value": format_time_in_timezones(after.timed_out_until.replace(tzinfo=pytz.utc)), "inline": False}
            ]
            if executor:
                fields.append({"name": "👮 Muted By", "value": f"{executor.mention} ({executor})", "inline": False})
            
            await log_action(
                title="🔇 Member Timed Out (External)",
                color=discord.Color.red(),
                fields=fields
            )
        else:
            fields = [{"name": "👤 User", "value": f"{after.mention} ({after})", "inline": False}]
            if executor:
                fields.append({"name": "🔓 Unmuted By", "value": f"{executor.mention} ({executor})", "inline": False})
            
            await log_action(
                title="🔓 Timeout Removed (External)",
                color=discord.Color.green(),
                fields=fields
            )

@bot.event
async def on_member_ban(guild, user):
    executor = None
    reason = None
    try:
        async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.ban):
            if entry.target.id == user.id:
                executor = entry.user
                reason = entry.reason
                break
    except:
        pass
    
    fields = [
        {"name": "👤 User", "value": f"{user.mention} ({user})", "inline": False},
        {"name": "🆔 User ID", "value": str(user.id), "inline": True}
    ]
    
    if executor:
        fields.append({"name": "🔨 Banned By", "value": f"{executor.mention} ({executor})", "inline": False})
    
    if reason:
        fields.append({"name": "📝 Reason", "value": reason, "inline": False})
    
    await log_action(
        title="🔨 Member Banned",
        color=discord.Color.dark_red(),
        fields=fields,
        dangerous=True
    )

@bot.event
async def on_member_unban(guild, user):
    executor = None
    try:
        async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.unban):
            if entry.target.id == user.id:
                executor = entry.user
                break
    except:
        pass
    
    fields = [
        {"name": "👤 User", "value": f"{user.mention} ({user})", "inline": False},
        {"name": "🆔 User ID", "value": str(user.id), "inline": True}
    ]
    
    if executor:
        fields.append({"name": "🔓 Unbanned By", "value": f"{executor.mention} ({executor})", "inline": False})
    
    await log_action(
        title="🔓 Member Unbanned",
        color=discord.Color.green(),
        fields=fields
    )

@bot.event
async def on_guild_channel_update(before, after):
    executor = None
    try:
        async for entry in after.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_update):
            if entry.target.id == after.id:
                executor = entry.user
                break
    except:
        pass
    
    changes = []
    
    if before.name != after.name:
        changes.append(f"**Name:** {before.name} → {after.name}")
    
    if hasattr(before, 'topic') and hasattr(after, 'topic') and before.topic != after.topic:
        changes.append("**Topic Changed**")
    
    if before.category != after.category:
        before_cat = before.category.name if before.category else "None"
        after_cat = after.category.name if after.category else "None"
        changes.append(f"**Category:** {before_cat} → {after_cat}")
    
    if before.overwrites != after.overwrites:
        changes.append("**Permissions Modified**")
    
    if changes:
        fields = [
            {"name": "Channel", "value": after.mention, "inline": False},
            {"name": "Changes", "value": "\n".join(changes), "inline": False}
        ]
        
        if executor:
            fields.append({"name": "✏️ Modified By", "value": f"{executor.mention} ({executor})", "inline": False})
        
        await log_action(
            title="✏️ Channel Updated",
            color=discord.Color.orange(),
            fields=fields
        )

@bot.event
async def on_bulk_message_delete(messages):
    executor = None
    try:
        guild = messages[0].guild if messages else None
        if guild:
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.message_bulk_delete):
                executor = entry.user
                break
    except:
        pass
    
    if len(messages) >= 20:
        file_content = f"Bulk Message Deletion Log\n"
        file_content += f"Total Messages: {len(messages)}\n"
        file_content += f"Channel: {messages[0].channel.name if messages else 'Unknown'}\n"
        if executor:
            file_content += f"Deleted By: {executor} (ID: {executor.id})\n"
        file_content += f"Time: {datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        file_content += f"Bot Used: {bot.user}\n"
        file_content += "\n" + "="*50 + "\n\n"
        
        for msg in messages:
            file_content += f"Author: {msg.author} (ID: {msg.author.id})\n"
            file_content += f"Time: {msg.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            file_content += f"Content: {msg.content}\n"
            if msg.attachments:
                file_content += f"Attachments: {', '.join([att.url for att in msg.attachments])}\n"
            file_content += "\n" + "-"*30 + "\n\n"
        
        file = discord.File(io.BytesIO(file_content.encode()), filename=f"purge_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        
        log_channel = bot.get_channel(MUTE_LOG_CHANNEL_ID)
        if log_channel:
            embed = discord.Embed(
                title="🗑️ Bulk Message Delete (Purge)",
                description=f"{len(messages)} messages were deleted",
                color=discord.Color.red(),
                timestamp=datetime.now(pytz.utc)
            )
            embed.add_field(name="📍 Channel", value=messages[0].channel.mention if messages else "Unknown", inline=False)
            
            if executor:
                embed.add_field(name="🗑️ Purged By", value=f"{executor.mention} ({executor})", inline=False)
            
            embed.add_field(name="📄 Full Log", value="See attached file for complete message history", inline=False)
            
            await log_channel.send(embed=embed, file=file)
            
            for user_id in DANGEROUS_LOG_USERS:
                try:
                    user = await bot.fetch_user(user_id)
                    file2 = discord.File(io.BytesIO(file_content.encode()), filename=f"purge_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
                    await user.send(embed=embed, file=file2)
                except:
                    pass
    else:
        message_info = []
        for msg in list(messages)[:10]:
            if not msg.author.bot:
                message_info.append(f"**{msg.author}**: {msg.content[:100]}")
        
        fields = [
            {"name": "📍 Channel", "value": messages[0].channel.mention if messages else "Unknown", "inline": False},
            {"name": "📝 Sample Messages", "value": "\n".join(message_info) if message_info else "No content", "inline": False}
        ]
        
        if executor:
            fields.append({"name": "🗑️ Purged By", "value": f"{executor.mention} ({executor})", "inline": False})
        
        await log_action(
            title="🗑️ Bulk Message Delete (Purge)",
            description=f"{len(messages)} messages were deleted",
            color=discord.Color.red(),
            fields=fields,
            dangerous=True
        )

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
        
        has_tracking_role = any(role.id in RCACHE_ROLES for role in member.roles)
        if not has_tracking_role:
            continue
        
        user_data = get_user_data(member.id)
        
        if user_data.get('last_message'):
            last_msg_time = datetime.fromisoformat(user_data['last_message']['timestamp'])
            time_since_msg = (now - last_msg_time).total_seconds()
            
            if time_since_msg <= 53:
                if user_data.get('online_start'):
                    user_data['total_online_seconds'] += 60
                    user_data['daily_seconds'] += 60
                    user_data['weekly_seconds'] += 60
                    user_data['monthly_seconds'] += 60
                else:
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
                if user_data.get('online_start'):
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
    
    for user_id, user_data in bot_data['users'].items():
        last_resets = user_data.get('last_reset', {})
        
        if last_resets.get('daily'):
            last_daily = datetime.fromisoformat(last_resets['daily'])
            if (now - last_daily).days >= 1:
                user_data['daily_seconds'] = 0
                user_data['last_reset']['daily'] = now.isoformat()
        
        if last_resets.get('weekly'):
            last_weekly = datetime.fromisoformat(last_resets['weekly'])
            if (now - last_weekly).days >= 7:
                user_data['weekly_seconds'] = 0
                user_data['last_reset']['weekly'] = now.isoformat()
        
        if last_resets.get('monthly'):
            last_monthly = datetime.fromisoformat(last_resets['monthly'])
            if (now - last_monthly).days >= 30:
                user_data['monthly_seconds'] = 0
                user_data['last_reset']['monthly'] = now.isoformat()
    
    save_data()

@bot.command(name='rhelp')
async def rhelp(ctx):
    if not has_mod_role(ctx.author):
        return
    
    embed = discord.Embed(
        title="📚 Bot Commands Help",
        description="Complete list of available commands",
        color=discord.Color.blue()
    )
    
    embed.add_field(name="!timetrack [user]", value="Shows detailed online/offline tracking stats", inline=False)
    embed.add_field(name="!rmute [users] [duration] [reason]", value="Mute multiple users", inline=False)
    embed.add_field(name="!runmute [user] [reason]", value="Unmute a user", inline=False)
    embed.add_field(name="!rmlb", value="RMute usage leaderboard", inline=False)
    embed.add_field(name="!rmal [moderator]", value="Show all mutes by a moderator", inline=False)
    embed.add_field(name="!rml", value="Show your mute history", inline=False)
    embed.add_field(name="!rcache", value="Show recently deleted messages", inline=False)
    embed.add_field(name="!tlb", value="Timetrack leaderboard (tracked roles)", inline=False)
    embed.add_field(name="!tdm", value="Timetrack leaderboard (non-tracked roles)", inline=False)
    embed.add_field(name="!sping / !hsping", value="Ping staff roles", inline=False)
    embed.add_field(name="!rdm", value="Toggle DM notifications", inline=False)
    
    embed.set_footer(text="Moderator-only commands")
    await ctx.send(embed=embed)

@bot.command(name='timetrack')
async def timetrack(ctx, member: discord.Member = None):
    if not has_mod_role(ctx.author):
        return
    
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
    
    if user_data.get('online_start'):
        online_duration = int((now - datetime.fromisoformat(user_data['online_start'])).total_seconds())
        embed.add_field(name="🟢 Status", value=f"Online for {format_duration(online_duration)}", inline=False)
    elif user_data.get('offline_start'):
        offline_duration = int((now - datetime.fromisoformat(user_data['offline_start'])).total_seconds())
        embed.add_field(name="🔴 Status", value=f"Offline for {format_duration(offline_duration)}", inline=False)
    else:
        embed.add_field(name="Status", value="Unknown", inline=False)
    
    if user_data.get('last_message'):
        last_msg = user_data['last_message']
        msg_time = datetime.fromisoformat(last_msg['timestamp'])
        embed.add_field(
            name="💬 Last Message",
            value=f"{last_msg.get('content', 'N/A')[:100]}\n\n{format_time_in_timezones(msg_time)}",
            inline=False
        )
    
    total = user_data.get('total_online_seconds', 0)
    daily = user_data.get('daily_seconds', 0)
    weekly = user_data.get('weekly_seconds', 0)
    monthly = user_data.get('monthly_seconds', 0)
    
    embed.add_field(name="📊 Total Time Online (All Time)", value=format_duration(total), inline=False)
    embed.add_field(name="📅 Today", value=format_duration(daily), inline=True)
    embed.add_field(name="📆 This Week", value=format_duration(weekly), inline=True)
    embed.add_field(name="📈 This Month", value=format_duration(monthly), inline=True)
    
    next_resets = get_next_reset_times(user_data)
    reset_info = []
    for period, reset_time in next_resets.items():
        time_until = reset_time - now
        reset_info.append(f"**{period.capitalize()}:** {format_duration(int(time_until.total_seconds()))}")
    
    embed.add_field(name="🔄 Next Resets In", value="\n".join(reset_info), inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name='rmute')
async def rmute(ctx, members: commands.Greedy[discord.Member], duration: str, *, reason: str):
    if not has_mod_role(ctx.author):
        return
    
    await ctx.message.delete()
    
    if not members:
        return
    
    mute_role = ctx.guild.get_role(MUTE_ROLE_ID)
    if not mute_role:
        return
    
    duration_seconds = parse_duration(duration)
    if duration_seconds == 0:
        return
    
    tracking_channel = bot.get_channel(MUTE_LOG_CHANNEL_ID)
    unmute_time = datetime.now(pytz.utc) + timedelta(seconds=duration_seconds)
    
    mod_id_str = str(ctx.author.id)
    bot_data['rmute_usage'][mod_id_str] = bot_data['rmute_usage'].get(mod_id_str, 0) + len(members)
    
    for member in members:
        await member.add_roles(mute_role, reason=reason)
        
        try:
            await member.timeout(timedelta(seconds=duration_seconds), reason=reason)
        except:
            pass
        
        bot_data['mutes'][str(member.id)] = {
            'moderator_id': ctx.author.id,
            'reason': reason,
            'duration': duration_seconds,
            'start_time': datetime.now(pytz.utc).isoformat(),
            'unmute_time': unmute_time.isoformat()
        }
        
        if mod_id_str not in bot_data['mute_history']:
            bot_data['mute_history'][mod_id_str] = []
        
        bot_data['mute_history'][mod_id_str].append({
            'user_id': member.id,
            'user_name': str(member),
            'reason': reason,
            'duration': duration_seconds,
            'timestamp': datetime.now(pytz.utc).isoformat()
        })
        
        user_id_str = str(member.id)
        if user_id_str not in bot_data['user_mute_history']:
            bot_data['user_mute_history'][user_id_str] = []
        
        bot_data['user_mute_history'][user_id_str].append({
            'reason': reason,
            'duration': duration_seconds,
            'timestamp': datetime.now(pytz.utc).isoformat(),
            'moderator_id': ctx.author.id
        })
        
        dm_embed = discord.Embed(
            title="🔇 You Have Been Muted",
            description=f"You have been muted in **{ctx.guild.name}**",
            color=discord.Color.red(),
            timestamp=datetime.now(pytz.utc)
        )
        dm_embed.add_field(name="⚠️ Reason", value=reason, inline=False)
        dm_embed.add_field(name="⏱️ Duration", value=format_duration(duration_seconds), inline=True)
        dm_embed.add_field(name="🕐 Unmute Time", value=format_time_in_timezones(unmute_time), inline=False)
        dm_embed.set_footer(text="Please follow the server rules.")
        
        await send_dm_safe(member, dm_embed)
        
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
        
        asyncio.create_task(auto_unmute(member, duration_seconds, reason, ctx.author))
    
    save_data()

async def auto_unmute(member: discord.Member, duration: int, original_reason: str, moderator: discord.Member):
    await asyncio.sleep(duration)
    
    mute_role = member.guild.get_role(MUTE_ROLE_ID)
    if mute_role and mute_role in member.roles:
        await member.remove_roles(mute_role, reason="Auto-unmute")
        
        try:
            await member.timeout(None, reason="Auto-unmute")
        except:
            pass
        
        if str(member.id) in bot_data['mutes']:
            del bot_data['mutes'][str(member.id)]
            save_data()
        
        dm_embed = discord.Embed(
            title="🔓 You Have Been Unmuted",
            description=f"Your mute in **{member.guild.name}** has expired",
            color=discord.Color.green(),
            timestamp=datetime.now(pytz.utc)
        )
        dm_embed.add_field(name="Original Reason", value=original_reason, inline=False)
        dm_embed.set_footer(text="Remember to follow server rules.")
        
        await send_dm_safe(member, dm_embed)
        
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
    if not has_mod_role(ctx.author):
        return
    
    mute_role = ctx.guild.get_role(MUTE_ROLE_ID)
    
    is_muted = (mute_role and mute_role in member.roles) or (member.timed_out_until and member.timed_out_until > datetime.now(pytz.utc))
    
    if not is_muted:
        await ctx.send("❌ This user is not muted.")
        return
    
    mute_data = bot_data['mutes'].get(str(member.id), {})
    
    if mute_role and mute_role in member.roles:
        await member.remove_roles(mute_role, reason=reason)
    
    try:
        await member.timeout(None, reason=reason)
    except:
        pass
    
    if mute_data.get('start_time'):
        start = datetime.fromisoformat(mute_data['start_time'])
        duration = int((datetime.now(pytz.utc) - start).total_seconds())
    else:
        duration = 0
    
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
    
    dm_embed = discord.Embed(
        title="🔓 You Have Been Unmuted",
        description=f"You have been unmuted in **{ctx.guild.name}**",
        color=discord.Color.green(),
        timestamp=datetime.now(pytz.utc)
    )
    dm_embed.add_field(name="Reason", value=reason, inline=False)
    
    await send_dm_safe(member, dm_embed)
    
    if str(member.id) in bot_data['mutes']:
        del bot_data['mutes'][str(member.id)]
        save_data()
    
    await ctx.send(f"✅ {member.mention} has been unmuted.")

@bot.command(name='rmlb')
async def rmlb(ctx):
    if not has_mod_role(ctx.author):
        return
    
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

@bot.command(name='rmal')
async def rmal(ctx, moderator: discord.Member = None):
    if not has_mod_role(ctx.author):
        return
    
    if moderator is None:
        moderator = ctx.author
    
    mod_id_str = str(moderator.id)
    mute_history = bot_data['mute_history'].get(mod_id_str, [])
    
    if not mute_history:
        await ctx.send(f"❌ No mute history found for {moderator.mention}")
        return
    
    embed = discord.Embed(
        title=f"📋 Mute Action List for {moderator.display_name}",
        description=f"Total mutes: {len(mute_history)}",
        color=discord.Color.blue(),
        timestamp=datetime.now(pytz.utc)
    )
    embed.set_thumbnail(url=moderator.display_avatar.url)
    
    for mute in mute_history[-10:]:
        mute_time = datetime.fromisoformat(mute['timestamp'])
        field_value = f"**Reason:** {mute['reason']}\n"
        field_value += f"**Duration:** {format_duration(mute['duration'])}\n"
        field_value += f"**Time:** {mute_time.strftime('%Y-%m-%d %H:%M UTC')}"
        
        embed.add_field(
            name=f"User: {mute['user_name']}",
            value=field_value,
            inline=False
        )
    
    if len(mute_history) > 10:
        embed.set_footer(text=f"Showing last 10 of {len(mute_history)} total mutes")
    
    await ctx.send(embed=embed)

@bot.command(name='rml')
async def rml(ctx):
    user_id_str = str(ctx.author.id)
    mute_history = bot_data['user_mute_history'].get(user_id_str, [])
    
    if not mute_history:
        await ctx.send("✅ You have no mute history!")
        return
    
    embed = discord.Embed(
        title=f"📋 Your Mute History",
        description=f"Total mutes: {len(mute_history)}",
        color=discord.Color.orange(),
        timestamp=datetime.now(pytz.utc)
    )
    embed.set_thumbnail(url=ctx.author.display_avatar.url)
    
    for mute in mute_history[-10:]:
        mute_time = datetime.fromisoformat(mute['timestamp'])
        field_value = f"**Reason:** {mute['reason']}\n"
        field_value += f"**Duration:** {format_duration(mute['duration'])}\n"
        field_value += f"**Time:** {mute_time.strftime('%Y-%m-%d %H:%M UTC')}"
        
        embed.add_field(
            name=f"Mute #{mute_history.index(mute) + 1}",
            value=field_value,
            inline=False
        )
    
    if len(mute_history) > 10:
        embed.set_footer(text=f"Showing last 10 of {len(mute_history)} total mutes")
    
    await ctx.send(embed=embed)

@bot.command(name='rcache')
async def rcache(ctx):
    if not has_mod_role(ctx.author):
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
        
        if msg.get('created_at'):
            created = datetime.fromisoformat(msg['created_at'])
            age = datetime.now(pytz.utc) - created.replace(tzinfo=pytz.utc)
            field_value += f"**Age:** {format_duration(int(age.total_seconds()))}\n"
        
        embed.add_field(name=f"Message {msg['id']}", value=field_value[:1024], inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name='tlb')
async def tlb(ctx):
    if not has_mod_role(ctx.author):
        return
    
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
    if not has_mod_role(ctx.author):
        return
    
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
    # Removed the moderator check - anyone can use this now
    
    await ctx.message.delete()
    
    staff_role = ctx.guild.get_role(STAFF_PING_ROLE)
    if not staff_role:
        return
    
    log_channels = [
        bot.get_channel(TRACKING_CHANNEL_ID),
        bot.get_channel(MUTE_LOG_CHANNEL_ID)
    ]
    
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
    
    await ctx.send(f"{staff_role.mention}")
    
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
    # Removed the moderator check - anyone can use this now
    
    await ctx.message.delete()
    
    higher_staff_role = ctx.guild.get_role(HIGHER_STAFF_PING_ROLE)
    if not higher_staff_role:
        return
    
    log_channels = [
        bot.get_channel(TRACKING_CHANNEL_ID),
        bot.get_channel(MUTE_LOG_CHANNEL_ID)
    ]
    
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
    
    await ctx.send(f"{higher_staff_role.mention}")
    
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

# Run the bot
if __name__ == "__main__":
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        print("❌ Error: DISCORD_TOKEN environment variable not set!")
        exit(1)
    
    keep_alive()
    bot.run(TOKEN)
