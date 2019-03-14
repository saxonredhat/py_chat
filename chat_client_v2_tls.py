import socket
import threading
import time
import struct
import sys
import ssl

try:
	host=sys.argv[1]
except:
	host='144.202.93.188'
try:
	port=int(sys.argv[2])
except:
	port=6900

username=u'张三'
password=u'1'

def send_msg(conn,msg):
	msg_len=struct.pack('H',len(bytes(msg,encoding='utf-8')))
	#conn.send(msg_len)
	conn.sendall(msg_len+bytes(msg,encoding='utf-8'))


def handle_input(conn):
	time.sleep(1)
	auth_msg=u'auth %s %s' % (username,password)
	print(auth_msg)
	send_msg(conn,auth_msg)
	while True:
		input_data=input('> ')
		send_msg(conn,input_data)
		
def handle_recv_msg(conn):
	while True:
		try:
			conn.settimeout(1.0)
			length=struct.unpack('H',conn.recv(2))[0]
			msg=conn.recv(int(length))
			if len(msg)!=length:
				print(u'客户端收到异常数据')
			print('')
			print(msg.decode('utf-8'),end='\n>')
		except:
			pass

def start_client():
	#生成SSL上下文

	#context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
	context=ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
	context.load_verify_locations('ssl/server.crt')
	with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as s:
		print(host,port)
		with context.wrap_socket(s,server_hostname='chat.unotes.co') as ssl_s:
			ssl_s.connect((host,port))
			t=threading.Thread(target=handle_recv_msg,args=(ssl_s,))
			t.setDaemon(True)
			t.start()
			#客户端认证
			time.sleep(0.2)
			#auth_msg=u'auth %s %s' % (username,password)
			#print(auth_msg)
			#send_msg(s,auth_msg)
			n=1
			while True:
				input_data=input('\n>')
				send_msg(ssl_s,input_data)
				if input_data == 'close':
					print("客户端正在关闭")
					break
				#input_data='gm 8 压力测试群,序号%s' % str(n)
		
if __name__ == '__main__':
	start_client()	
