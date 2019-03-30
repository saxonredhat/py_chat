import asyncio
import aiomysql
import aioredis

import sys
import struct
import ssl

import time
import logging


SERVER_ADDRESS = ('0.0.0.0',10000)
logging.basicConfig(
	level=logging.DEBUG,
	format='%(name)s: %(message)s',
	stream=sys.stderr,
)
log=logging.getLogger('main')

#LOOP
loop=asyncio.get_event_loop()


#TLS配置
CERTFILE='ssl/server.crt'
KEYFILE='ssl/server.key'


#在线用户userid到读写Stream映射
auth_userid_map_wstream={}
auth_wstream_map_userid={}


#mysql 配置
db_config={
	'host':'localhost',
	'port':3306,
	'user':'root',
	'password':'Sys20180808!',
	'db':'chat',
	'charset':'utf8'
}


#SQL 模板
QUERY_AUTH_SQL='select password from user where id=%s'
QUERY_USERID_IS_EXISTS_SQL='select id from user where id=%s'
QUERY_USERNAME_OF_USERID_SQL='select username from user where id=%s'


#redis配置
#redis_host='localhost'
#redis_port=6379
redis_unix_socket='/var/run/redis/redis.sock'


#redis KEY模板
KV_EXISTS_USERID=u'kv_exists_userid:userid:%s'
KV_USERID_GET_USERNAME=u'kv_userid_get_username:userid:%s'
LIST_USERMESSAGES_OF_USERID=u'list_usermessages_of_userid:userid:%s'


#支持的命令列表
cmd_list=['auth','msg','logout']


#命令帮助
cmd_help="""
     ===============================================
    |                                               |
    |                命令使用说明:                  |
    |                                               |
     ===============================================
    |命令          参数   参数     (说明)           |
     ===============================================
    |auth          UID    PASSWORD (登录系统)       |
    |msg/m         UID    MESSAGE  (给用户发送消息) |
     ===============================================
    |命令                (说明)                     |
     ===============================================
    |logout              (退出)                     |
     ===============================================
"""



async def get_custom_time_string(t_format="%Y-%m-%d %X"):
	"""获取指定格式的时间字符串
	   默认格式为"%Y-%m-%d %X"
	"""
	return time.strftime(t_format)


async def get_db_pool():
	"""获取mysql连接池"""
	pool = await aiomysql.create_pool(**db_config)
	return pool


async def get_redis_pool():
	"""获取redis连接池"""
	pool = await aioredis.create_pool(
		redis_unix_socket,
		minsize=5, maxsize=10)
	return pool


async def db_query(sql):
	"""mysql执行查询操作"""
	pool = await get_db_pool() 
	results=()
	async with pool.acquire() as conn:
		async with conn.cursor() as cur:
			log.debug(f'查询数据库,SQL->[ {sql} ]')
			await cur.execute(sql)
			results = await cur.fetchall()
	pool.close()
	await pool.wait_closed()

	return results


async def redis_query(cmd,*args):
	"""redis执行查询操作"""
	pool=await get_redis_pool()
	with await pool as conn:
		results =await conn.execute(cmd,*args)
	pool.close()
	await pool.wait_closed()

	return results


async def redis_dml(cmd,*args):
	"""redis执行增删改操作"""
	pool=await get_redis_pool()
	with await pool as conn:
		await conn.execute(cmd,*args)
	pool.close()
	await pool.wait_closed()


async def get_username_of_userid(userid):
	"""获取userid对应的用户名"""
	if await redis_query('exists',KV_USERID_GET_USERNAME % userid):
		username=await redis_query('get',KV_USERID_GET_USERNAME % userid)
		return username.decode('utf-8')
	else:
		res=await db_query(QUERY_USERNAME_OF_USERID_SQL % userid)
		if res:
			username=res[0][0]
			await redis_dml('set',KV_USERID_GET_USERNAME % userid,username)
			return username 
	return ''


async def get_userid_of_wstream(wstream):
	"""通过writestream获取userid"""
	try:
		userid=auth_wstream_map_userid[wstream]
		return userid 
	except KeyError:
		return -1


async def get_wstream_of_userid(userid):
	"""通过userid获取writestream"""
	try:
		write_stream=auth_userid_map_wstream[userid]
		return write_stream 
				
	except KeyError:
		return ''	


async def write_data(writer,data):
	"""发送数据"""
	b_data=bytes(data,encoding='utf-8')
	b_datasize=struct.pack('H',len(b_data))
	try:
		writer.write(b_datasize+b_data)
		await writer.drain()	
		return True
	except:
		await clean_wstream(writer)	
		return False


async def read_data(reader): 
	"""读取数据"""
	try:
		length=struct.unpack('H',await reader.read(2))[0]
		b_data=await reader.read(int(length))
		return b_data.decode('utf-8') 
	except Exception as e:
		log.debug(f'读取客户端数据异常:[{e}]')

	return ''


async def clean_wstream(writer):
	"""清理客户端登陆信息和关闭stream"""
	userid=await get_userid_of_wstream(writer)
	if userid != -1:
		try:
			del auth_userid_map_wstream[userid]
		except:
			pass
		try:
			del auth_wstream_map_userid[writer]
		except:
			pass
	writer.close()


async def user_auth(*args):
	"""用户认证"""
	userid=int(args[0])
	pwd=args[1]

	sql=QUERY_AUTH_SQL % (userid)
	res=await db_query(sql)
	if res:
		if res[0][0] == pwd:	
			return True
	return False


async def userid_is_exists(userid):
	""""判断userid是否存在"""
	if await redis_query('exists',KV_EXISTS_USERID % userid):
		return True
	else:
		res=await db_query(QUERY_USERID_IS_EXISTS_SQL % userid)
		if res:
			await redis_dml('set',KV_EXISTS_USERID % userid,'')
			return True

	return False


async def userid_is_online(userid):
	"""判断用户是否在线"""
	try:
		writer=auth_userid_map_wstream[userid]
		return True 
	except KeyError:
		return False


async def offline_userid(operator_type,*args):
	"""下线用户"""
	userid=int(args[0])
	try:
		writer=auth_userid_map_wstream[userid]

		if operator_type == 0:
			msg='有用户登录您的账号,您已被挤下线！'
		elif operator_type == 1:
			msg='您已退出登录！'
		else:
			msg='您已退出系统！'
		await write_data(writer,msg)

	except Exception as e:
		log.debug(e)
	finally:
		try:
			del auth_userid_map_wstream[userid]
		except:
			log.debug('auth_userid_map_wstream 删除失败')
		try:
			del auth_wstream_map_userid[writer]
		except:
			log.debug('auth_wstream_map_userid 删除失败')
			

async def send_user_msg(writer,*args):
	"""发送用户消息"""
	to_userid=int(args[0])
	from_userid=auth_wstream_map_userid[writer]
	content=' '.join(args[1:])

	if not await userid_is_exists(to_userid):
		msg='系统不存在该userid!'	
		await write_data(writer,msg)
		return

	from_username=await get_username_of_userid(from_userid)
	to_username=await get_username_of_userid(to_userid)
	custom_time=await get_custom_time_string()
	send_content=f'{custom_time}\n[ {from_username}|UID:{from_userid} ]:{content}'

	if to_userid == from_userid:
		send_content=f'{custom_time}\n[ {from_username}|UID:{from_userid} ]:{content}'
		await write_data(writer,send_content)
		return

	if await userid_is_online(to_userid):
		to_writer=auth_userid_map_wstream[to_userid]
		if await write_data(to_writer,send_content):
			await write_data(writer,send_content)
			return 
	#用户不在线，发送离线消息
	print("用户不在线，发送离线消息")
	await redis_dml('rpush',LIST_USERMESSAGES_OF_USERID % to_userid,send_content)
	await write_data(writer,f'[离线消息]\n{send_content}')


async def user_is_auth(writer):
	"""判断用户是否认证"""
	if writer in auth_wstream_map_userid.keys():
		return True
	return False


async def get_offline_msg_of_userid(userid):
	"获取userid对应的离线消息"
	return await redis_query('lrange',LIST_USERMESSAGES_OF_USERID % userid,0,-1)


async def push_offline_msg(writer,*args):
	"""推送离线消息"""
	userid=int(args[0])
	user_offline_messages_list=await get_offline_msg_of_userid(userid)
	for user_offline_message in user_offline_messages_list:
		await write_data(writer,user_offline_message.decode('utf-8'))
	await redis_query('del',LIST_USERMESSAGES_OF_USERID % userid)


async def update_auth(writer,*args): 
	"""更新auth_userid_map_wstream和auth_wstream_map_userid"""
	userid=int(args[0])
	auth_userid_map_wstream[userid]=writer
	auth_wstream_map_userid[writer]=userid


async def check_args_validity(writer,cmd,*args):
	if cmd == 'auth':
		if len(args) != 2:
			msg='提示:认证格式: [ auth userid pwd ]'
			await write_data(writer,msg)
			return False
		try:
			userid=int(args[0])
		except:
			msg='userid为整数!'
			await write_data(writer,msg)
			return False

	elif cmd == 'msg':
		if len(args) < 2:
			msg='提示:消息格式: [ msg userid content ]'
			await write_data(writer,msg)
			return False
		try:
			to_userid=int(args[0])
		except:
			msg='userid为整数!'
			await write_data(writer,msg)
			return False
			
	return True
		

async def handler_client(reader,writer):
	"""处理客户端连接"""
	address=writer.get_extra_info('peername')
	log=logging.getLogger('echo_{}_{}'.format(*address))
	log.debug('收到新连接')
	args_ok=True
	cmd_ok=True
	while True:
		if not await user_is_auth(writer) and cmd_ok and args_ok:
			msg=u'请登陆！'
			await write_data(writer,msg)

		data=await read_data(reader)
		if not data:
			break
		cmd=data.split(' ')[0]
		args=data.split(' ')[1:]

		if cmd == '?' or cmd == 'help':
			await write_data(writer,f'{cmd_help}')
			cmd_ok=False
			continue
		elif cmd not in cmd_list:
			await write_data(writer,'输入的命令不支持,请使用 help 或 ? 查看帮助!')
			cmd_ok=False
			continue

		cmd_ok=True

		if not await user_is_auth(writer):
			if cmd != 'auth':
				continue

			if not await check_args_validity(writer,cmd,*args):
				args_ok=False
				continue
			args_ok=True
			if await user_auth(*args):
				await offline_userid(0,*args)

				await update_auth(writer,*args)

				msg='认证成功!'
				await write_data(writer,msg)

				await push_offline_msg(writer,*args)
			else:
				msg='认证失败!'
				await write_data(writer,msg)
			
		else:
			if not await check_args_validity(writer,cmd,*args):
				args_ok=False
				continue
			args_ok=True
			if cmd == 'msg':
				await send_user_msg(writer,*args)

			elif cmd == 'logout':
				userid=await get_userid_of_wstream(writer)
				await offline_userid(1,userid)

			elif cmd == 'auth':
				await write_data(writer,'您已登录！')


context=ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
context.load_cert_chain(certfile=CERTFILE,keyfile=KEYFILE)
factory=asyncio.start_server(handler_client,*SERVER_ADDRESS,ssl=context)
server=loop.run_until_complete(factory)
log.debug('starting up on {} port {}'.format(*SERVER_ADDRESS))


try:
	loop.run_forever()
except KeyboardInterrupt:
	pass
finally:
	log.debug('关闭服务器.')
	server.close()
	loop.run_until_complete(server.wait_closed())

	log.debug('关闭事件循环LOOP.')
	loop.close()
