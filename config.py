import datetime
from os import environ 

#Dont Remove My Credit @Silicon_Bot_Update 
#This Repo Is By @Silicon_Official 
# For Any Kind Of Error Ask Us In Support Group @Silicon_Botz 

class Config:
    API_ID = environ.get("API_ID", "29453152")
    API_HASH = environ.get("API_HASH", "2302adc174dbc954ae5081eda5131166")
    BOT_TOKEN = environ.get("BOT_TOKEN", "") 
    BOT_SESSION = environ.get("BOT_SESSION", "Auto_Forward") 
    DATABASE_URI = environ.get("DATABASE", "mongodb+srv://gd3251791_db_user:GDPQbmyXAEFDGpbL@cluster0.6jxsnxc.mongodb.net/?appName=Cluster0")
    DATABASE_NAME = environ.get("DATABASE_NAME", "forwardbot")
    BOT_OWNER_ID = [int(id) for id in environ.get("BOT_OWNER_ID", '1052054451').split()]
    LOG_CHANNEL = int(environ.get('LOG_CHANNEL', ''))
    FORCE_SUB_CHANNEL = environ.get("FORCE_SUB_CHANNEL", "https://t.me/+ulUxyWr94VkzZTE9") 
    FORCE_SUB_ON = environ.get("FORCE_SUB_ON", "True")
    PORT = environ.get('PORT', '8080')

    # ── Member-channel forward (userbot) ─────────────────────────────────────
    # Pyrogram session string for a personal Telegram account (NOT a bot).
    # Powers /member_forward, which pulls files out of channels where this
    # account is a member/admin but the BOT ITSELF was never added — the Bot
    # API can never do that on its own. Leave empty to disable the feature.
    SESSION_STRING = environ.get("SESSION_STRING", "BQHBa2AAB2Gkf7fVzKe7laAj3-sVJdoVgs7kdqElm_ivE4bUGIML4SNioZOtM_oBIk-Gal_oszjfAT7QIumIVsCMXVuyD0Gh29p1204DwCQ03-H28cieNGmi7-q75p0LETReT3xm54yhXKu1lfcpwu5eNMs9YeI9uPD2yeplb1ma3HyEFnTgJLSGXSR6Ww2EcNvVvum25FElPQlQ___oEdfTMygfTOmILhxkk3ehTTg1a0TrbfdGooam7-1eggRmFHw4kOQbjWRIvvVegOwlt-PZEfHYBviqr0KQftEAjSJ2pS6kvVM5qioOyGbSK8iIKraNBRp6SWv9JZpkDxyRagtMhQbtaAAAAAHGKGRSAA")
    
#Dont Remove My Credit @Silicon_Bot_Update 
#This Repo Is By @Silicon_Official 
# For Any Kind Of Error Ask Us In Support Group @Silicon_Botz 

   
class temp(object): 
    lock = {}
    CANCEL = {}
    forwardings = 0
    BANNED_USERS = []
    IS_FRWD_CHAT = []
    
#Dont Remove My Credit @Silicon_Bot_Update 
#This Repo Is By @Silicon_Official 
# For Any Kind Of Error Ask Us In Support Group @Silicon_Botz
