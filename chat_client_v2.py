import socket
import threading
import time
import struct
import sys

try:
	host=sys.argv[1]
except:
	host='144.202.93.188'
try:
	port=int(sys.argv[2])
except:
	port=6900

username=u'张三'
password=u'123456'

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
		conn.settimeout(1.0)
		try:
			length=struct.unpack('H',conn.recv(2))[0]
			msg=conn.recv(int(length))
			if len(msg)!=length:
				print(u'客户端收到异常数据')
			print('')
			print(msg.decode('utf-8'),end='\n>')
		except:
			pass

def start_client():
	with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as s:
		print(host,port)
		s.connect((host,port))
		t=threading.Thread(target=handle_recv_msg,args=(s,))
		t.start()
		#客户端认证
		time.sleep(0.2)
		#auth_msg=u'auth %s %s' % (username,password)
		#print(auth_msg)
		#send_msg(s,auth_msg)
		while True:
			input_data=input('\n>')
			send_msg(s,input_data)
	
if __name__ == '__main__':
	start_client()	
