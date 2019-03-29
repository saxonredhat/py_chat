import asyncio
from aioconsole import ainput
import concurrent.futures

import sys
import ssl
import time
import struct

import logging


#日志配置
logging.basicConfig(
	level=logging.DEBUG,
	format='%(name)s: %(message)s',
	stream=sys.stderr,
)
log = logging.getLogger('main')


#TLS配置
CERTFILE='ssl/server.crt'


#chat服务器配置
SERVER_ADDRESS = ('chat.unotes.co',10000)


loop = asyncio.get_event_loop()


async def write_data(writer,data):
	"""发送数据"""
	b_data=bytes(data,encoding='utf-8')
	b_datasize=struct.pack('H',len(b_data))
	try:
		writer.write(b_datasize+b_data)
		await writer.drain()
		return True
	except:
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


async def read(reader,once=False):
	"""循环读取数据"""
	while True:
		data=await read_data(reader)
		if data:
			if once:
				print(f'{data}\n',end='')
				break
			print(f'\x08\x08\x08{data}\n>>>',end='')
			

async def chat_client(address):
	"""聊天客户端"""
	log = logging.getLogger('chat_client')
	log.debug('connecting to {} port {}'.format(*address))
	context=ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
	context.load_verify_locations(CERTFILE)
	reader, writer = await asyncio.open_connection(*address,ssl=context)
	await read(reader,True)
	read_task=asyncio.create_task(read(reader))
	while True:
		data=await ainput('>>>')
		if not data:
			data='?'
		await write_data(writer,data) 


try:
	loop.run_until_complete(
		chat_client(SERVER_ADDRESS)
	)
except:
	pass
finally:
	log.debug('closing event loop')
	loop.close()
