#coding:utf-8
######################
# 作者: SheYinsong   #
# 时间: 2019-03-12   #
######################
import sys
import socket
import time
import threading
import struct
import pymysql
import selectors
import select
import queue
import ssl
import redis

#数据库配置
db_config={
	'host':'localhost',
	'port':3306,
	'user':'root',
	'passwd':'Sys20180808!',
	'db':'chat',
	'charset':'utf8'
}

#redis配置
r_redis=redis.Redis(host='localhost',port=6379,db=0)
#redis键值模板
KV_EXISTS_GROUPID=u'kv_exists_groupid:groupid:%s'
KV_NOSPEAKING_GROUPID=u'kv_nospeaking_groupid:groupid:%s'
KV_NOSPEAKING_USERID_IN_GROUPID=u'kv_nospeaking_userid_in_groupid:groupid:%s:userid:%s'
KV_GROUPID_GET_GROUPNAME=u'kv_groupid_get_groupname:groupid:%s'
KV_GROUPID_GET_OWN_USERID=u'kv_groupid_get_own_userid:groupid:%s'
KV_USERID_IN_GROUPID=u'kv_userid_in_groupid:userid:%s:groupid:%s'
KV_OWNUSERID_OWN_GROUPID=u'kv_ownuserid_own_groupid:own_userid:%s:groupid:%s'
KV_USERID_GET_USERNAME=u'kv_userid_get_username:userid:%s'
KV_USERNAME_GET_USERID=u'kv_username_get_userid:username:%s'
KV_USERID_ISFRIEND_USERID=u'kv_userid_isfriend_userid:userid:%s:userid:%s'
LIST_GROUPMESSAGES_OF_USERID_IN_GROUPID=u'list_groupmessages_of_userid_in_groupid:groupid:%s:userid:%s'
LIST_USERIDS_OF_GROUPID=u'list_userids_of_groupid:groupid:%s'
LIST_USERMESSAGES_OF_USERID=u'list_usermessages_of_userid:userid:%s'

#TLS配置
CERTFILE='ssl/server.crt'
KEYFILE='ssl/server.key'

#监听配置
host='0.0.0.0'
try:
	port=int(sys.argv[1])
except:
	port=6900

#无参数命令
noargs_cmd=['listfriends','lf','listgroup','lg','listallgroup','lag','close','logout']

#参数命令
args_cmd=['msg','m','gmsg','gm','adduser','au','deluser','du','entergroup','eng','exitgroup','exg','kickout','ko','reject','rj','accept','ac','creategroup','cg','delgroup','dg','listgroupusers','lgu','auth','echo','nospkuser','nsu','spkuser','su','nospkgroup','nsg','spkgroup','sg']

#命令帮助
cmd_help="""
	 ===============================================
	|                                               |
	|                命令使用说明:                  |
	|                                               |
	 ===============================================
	|命令          参数1    参数2   (说明)          | 
	 ===============================================
	|auth          用户名   密码    (登录系统)      |
	|msg/m         用户名   消息    (给用户发送消息)|
	|gmsg/gm       群ID     消息    (给群发送消息)  |
	|kickout/ko    群ID     用户    (移出群用户)    |
	|nospkuser/nsu 群ID     用户    (禁言群用户)    |
	|spkuser/su    群ID     用户    (取消禁言群用户)|
	 ===============================================
	|命令                参数    (说明)             |
	 ===============================================
	|echo                消息    (回显消息)         |
	|adduser/au          用户名  (添加好友)         |
	|deluser/du          用户名  (删除好友)         |
	|creategroup/cg      群名    (建群)             |
	|delgroup/dg         群ID    (删群)             |
	|entergroup/eng      群ID    (加群)             |
	|exitgroup/exg       群ID    (退群)             |
	|nospkgroup/nsg      群ID    (禁言群)           |
	|spkgroup/sg         群ID    (取消禁言群)       |
	|reject/rj           请求ID  (同意加好友|进群)  |
	|accept/ac           请求ID  (拒绝加好友|进群)  |
	|listgroupusers/lgu  群ID    (列出群成员信息)   |
	 ===============================================
	|命令                (说明)                     |
	 ===============================================
	|listfriends/lf      (列出好友)                 |
	|listgroup/lg        (列出创建的群|列出加入的群)|
	|listallgroup/lag    (列出系统所有的群)         |
	|logout              (退出)                     |
	|close               (关闭对话)                 |
	 ===============================================
"""

#活动的客户端socket列表
userid_to_socket={}

#登陆成功并且没退出的socket列表
auth_fd_to_socket={}

#在线socket列表
fd_to_socket={}

#命令统计
fd_to_command_count={}

#消息字段队列
message_queue={}

#创建epoll
epoll=select.epoll()


def get_conn_userid(conn):
	for userid,conn_user in userid_to_socket.items():
		if conn == conn_user:
			return userid
	return -1

def del_online_sock(conn):
	userid=get_conn_userid(conn)
	if userid>0:
		del userid_to_socket[userid]

def send_msg(conn,msg):
	msg_len=struct.pack('H',len(bytes(msg,encoding='utf-8')))
	try:
		conn.sendall(msg_len+bytes(msg,encoding='utf-8'))
		return True
	except:
		conn.close()
		return False

def sql_query(sql):
	db=pymysql.connect(**db_config)
	cursor=db.cursor()
	results=''
	try:
		print(u'[ %s ] 数据执行查询操作! %s' % (time.strftime("%Y-%m-%d %X"),sql))	
		cursor.execute(sql)
		results=cursor.fetchall()
	except Exception as e:
		print(u'[ %s ] 数据执行查询异常! %s' % (time.strftime("%Y-%m-%d %X"),e))	
	finally:
		cursor.close()
		db.close()
	return results

def sql_dml(sql):
	db=pymysql.connect(**db_config)
	cursor=db.cursor()
	try:
		print(u'[ %s ] 数据执行DML操作! %s' % (time.strftime("%Y-%m-%d %X"),sql))	
		cursor.execute(sql)	
		db.commit()
		return True
	except Exception as e:
		print(u'[ %s ] 数据库执行DML操作异常！%s,进行回滚操作！' % (time.strftime("%Y-%m-%d %X"),e))
		db.rollback()
		return False
	finally:
		cursor.close()
		db.close()

def user_userid_name(userid):
	sql='select username from user where id=%s' % userid
	res=sql_query(sql)
	if not res:
		return ''
	return res[0][0]

def user_name_userid(username):
	sql='select id from user where username="%s"' % username
	res=sql_query(sql)
	if not res:
		return -1 
	return res[0][0]

def get_userid(username):
	sql='select id from user where username="%s" LIMIT 1' % username
	try:
		userid=sql_query(sql)[0][0]
	except:
		userid=-1
	return userid

def user_is_online(userid):
	for uid,conn_user in userid_to_socket.items():
		if userid == uid:
			return True
	return False

def get_userid_conn(userid):
	print(userid_to_socket)
	for uid,conn_user in userid_to_socket.items():
		if int(userid) == uid:
			return conn_user

def user_auth(user,pwd):
	sql="select password from user where username='%s' LIMIT 1" % user
	try:
		res=sql_query(sql)
	except:
		print(u'[ %s ] 数据库执行异常，退出认证！' % time.strftime("%Y-%m-%d %X"))
		return False
	if not res:
		print(u'[ %s ] 客户端认证用户名%s不存在！' % (time.strftime("%Y-%m-%d %X"),user))
		return False
	try:
		q_pwd=res[0][0]
	except:
		print(u'[ %s ] 获取数据异常！' % time.strftime("%Y-%m-%d %X"))
		return False
		
	if q_pwd != pwd:
		print(u'[ %s ] 客户端认证用户%s密码错误！' % (time.strftime("%Y-%m-%d %X"),user))
		return False
	print("用户[ %s ]认证成功" % user)
	return True 

def get_data(conn):
	#获取2个字节数据,为数据的大小
	length=struct.unpack('H',conn.recv(2))[0]
	data=conn.recv(int(length))
	if len(data)!=length:
		print(u'[ %s ] 客户端数据包异常！' % (time.strftime("%Y-%m-%d %X")))
		data_err_msg=u'【系统提示】认证信息异常'
		send_msg(conn,data_err_msg)
		return
	return data

def user_conn_auth(conn,cli_ip,cli_port):
	#获取2个字节数据,为数据的大小
	length=struct.unpack('H',conn.recv(2))[0]
	data=conn.recv(int(length))
	if len(data)!=length:
		print(u'[ %s ] 客户端(%s:%s)认证异常' % (time.strftime("%Y-%m-%d %X"),cli_ip,cli_port))
		auth_err_msg=u'【系统提示】认证信息异常'
		send_msg(conn,auth_err_msg)
		return False
	else:
		#获取命令
		cmd=data.decode('utf-8').split(' ')[0]
		if cmd != 'auth':
			print(u'[ %s ] 客户端(%s:%s)认证命令格式错误:%s' % (time.strftime("%Y-%m-%d %X"),cli_ip,cli_port,cmd))
			auth_format_err_msg=u'【系统提示】认证命令格式错误！\n认证命令:auth username password'
			send_msg(conn,auth_format_err_msg)	
			return False
		else:
			user=msg.decode('utf-8').split(' ')[1]
			pwd=msg.decode('utf-8').split(' ')[2]
			sql="select id,username,password from user where username='%s' LIMIT 1" % user
			res=sql_query(sql)
			if not res:
				print(u'[ %s ] 客户端认证用户名%s不存在！' % (time.strftime("%Y-%m-%d %X"),user))
				auth_user_not_exist_err_msg=u'【系统提示】客户端认证用户名不存在!'
				send_msg(conn,auth_user_not_exist_err_msg)
				return False
			if res[0][2]!=pwd:
				print(u'[ %s ] 客户端(%s:%s)认证用户%s密码错误！' % (time.strftime("%Y-%m-%d %X"),user))
				auth_pwd_err_msg=u'【系统提示】用户名或者密码错误!'
				send_msg(conn,auth_pwd_err_msg)
				return False
			return True
			
def push_new_msg(conn,user_id):
	#延时0.2秒
	time.sleep(0.2)
	#获取用户拥有的群的id
	sql='select groupid from group_users where userid=%s' % user_id
	groupid_list=[]
	res=sql_query(sql)
	for r in res:
		groupid=r[0]
		groupid_list.append(r[0])

	all_msg_list=[]
	#是否收到群消息
	for groupid in groupid_list:
		#判断是否需要推送给当前用户
		r_key=LIST_GROUPMESSAGES_OF_USERID_IN_GROUPID % (groupid,user_id)
		l_len=r_redis.llen(r_key)
		for seq in range(0,l_len):
			gmsg=r_redis.lpop(r_key).decode('utf-8')
			all_msg_list.append(gmsg)
			#send_msg(conn,gmsg)
				
	#是否收到用户消息
	#查询redis数据库是否有该用户的数据
	r_key=LIST_USERMESSAGES_OF_USERID % user_id
	msg_count=r_redis.llen(r_key)
	for i in range(0,msg_count):	
		msg=r_redis.lpop(r_key).decode('utf-8')
		all_msg_list.append(msg)
		#send_msg(conn,msg)

	#是否收到提醒消息
	sql='select id,content from user_notice where status=0 and userid=%s' % user_id  
	res=sql_query(sql)
	if res:
		for r in res:
			all_msg_list.append(r[1])	
			#send_msg(conn,r[1])
			sql='update user_notice set status=1 where id=%s' % r[0]
			sql_dml(sql)
	
	for msg in sorted(all_msg_list):
		send_msg(conn,msg)
	
	#是否收到加好友消息
	sql='select id,userid from user_req where add_userid=%s and status=0' % user_id 
	res=sql_query(sql)
	if res:
		for r in res:
			req_id=r[0]
			req_userid=r[1]
			req_username=user_userid_name(req_userid)
			send_msg(conn,u"[ %s ]\n【系统消息】用户%s向您申请添加好友请求!\n请求号为%s\n同意：accept %s \n拒绝：reject %s" % (time.strftime("%Y-%m-%d %X"),req_username,req_id,req_id,req_id))

	#是否收到加群消息
	sql='select ur.id,ur.userid,g.id,g.name from user_req ur join `group` g on g.id=ur.add_groupid where g.own_userid=%s and ur.status=0' % user_id
	res=sql_query(sql)
	if res:
		for r in res:
			req_id=r[0]
			req_userid=r[1]
			req_username=user_userid_name(req_userid)
			groupid=r[2]
			group_name=r[3]
			send_msg(conn,u'[ %s ]\n【系统消息】用户[ %s ]申请加入群[ %s|ID:%s ]\n申请号:%s\n同意：accept %s \n拒绝：reject %s' % (time.strftime("%Y-%m-%d %X"),req_username,group_name,groupid,req_id,req_id,req_id))

def user_msg(conn,args):
	from_userid=get_conn_userid(conn)
	#加载redis中的数据
	r_key=KV_USERID_GET_USERNAME % from_userid 
	if r_redis.exists(r_key) and r_redis.get(r_key):
		from_username=r_redis.get(r_key).decode('utf-8')
	#redis不存在对应username
	else:
		sql='select username from user where id=%s' % from_userid 
		res=sql_query(sql)
		if not res:
			print(u'[ %s ] 用户id[ %s ]在系统中不存在' % (time.strftime("%Y-%m-%d %X"),from_userid))	
			send_msg(conn,u'【系统消息】 用户id[ %s ]在系统中不存在' % from_userid)
			return
		from_username=res[0][0]
		#把结果写入redis
		r_redis.set(r_key,from_username)
	to_username=args[0]
	content=' '.join(args[1:])

	#加载redis中的数据
	r_key=KV_USERNAME_GET_USERID % to_username 
	if r_redis.exists(r_key) and r_redis.get(r_key):
		to_userid=r_redis.get(r_key).decode('utf-8')
	#redis不存在对应username
	else:
		sql='select id from user where username="%s"' % to_username
		res=sql_query(sql)
		if not res:
			print(u'[ %s ] 用户%s在系统中不存在' % (time.strftime("%Y-%m-%d %X"),to_username))	
			send_msg(conn,u'【系统消息】 用户%s在系统中不存在' % to_username)
			return	
		to_userid=res[0][0]
		#把结果写入redis
		r_redis.set(r_key,to_userid)

	#判断是否为自己发送给自己
	print("from_userid,to_userid",type(from_userid),type(to_userid))
	if from_userid == int(to_userid):
		user_msg=u'[ %s ]\n【好友消息】[ %s ]: %s' % (time.strftime("%Y-%m-%d %X"),from_username,content)
		send_msg(conn,user_msg)
		return
	#判断当前发送消息的接收方是否为好友，如果不是拒绝发送消息
	#加载redis中的数据
	r_key=KV_USERID_ISFRIEND_USERID % (from_userid,to_userid)
	is_friend=r_redis.exists(r_key)
	if not is_friend:
		sql='select userid from user_users where userid=%s and friend_userid=%s' % (from_userid,to_userid)
		res=sql_query(sql)
		if not res:
			send_msg(conn,u'【系统消息】 该用户还不是您的好友！不能发送消息')
			return 
		 #把结果写入redis
		r_redis.set(r_key,"")
	to_conn=get_userid_conn(to_userid)
	send_time=time.strftime("%Y-%m-%d %X")
	user_msg=u'[ %s ]\n【好友消息】【%s】: %s' % (send_time,from_username,content)
	if to_conn:
		print("对方在线！！！！")
		try:
			send_msg(to_conn,user_msg)
		except:
			r_key=LIST_USERMESSAGES_OF_USERID % to_userid
			r_redis.rpush(r_key,user_msg) 
			print("[redis写入]%s-->%s" % (r_key,user_msg))
	else:
		print("对方不在线！！！！")
		r_key=LIST_USERMESSAGES_OF_USERID % to_userid
		r_redis.rpush(r_key,user_msg) 
		print("redis写入:%s->%s" % (r_key,user_msg))
	user_msg=u'[ %s ]\n【发送消息】->【%s】（好友）: %s' % (send_time,to_username,content)
	send_msg(conn,user_msg)

def user_gmsg(conn,args):
	req_userid=get_conn_userid(conn)
	req_username=user_userid_name(req_userid)
	args_list=args
	if len(args_list)<2:
		send_msg(conn,u'【系统提示】发群消息的操作参数错误')
		return	
	groupid=args_list[0]
	content=' '.join(args_list[1:])

	#判断群是否存在
	#读取redis	
	r_key=KV_EXISTS_GROUPID % groupid 
	if not r_redis.exists(r_key):
		sql='select id,own_userid,name from `group` where id=%s' % groupid 
		res=sql_query(sql)
		if not res:
			send_msg(conn,"【系统提示】该群ID号[ %s ]不存在！" % groupid)
			return 
		r_redis.set(r_key,"")

	#获取群主ID
	#读取redis	
	r_key=KV_GROUPID_GET_OWN_USERID % groupid
	if r_redis.exists(r_key) and r_redis.get(r_key):
		own_userid=r_redis.get(r_key).decode('utf-8')
	else:
		sql='select own_userid from `group` where id=%s' % groupid
		res=sql_query(sql)
		if res:
			own_userid=res[0][0]
			r_redis.set(r_key,own_userid)

	#读取redis	
	r_key=KV_GROUPID_GET_GROUPNAME % groupid
	if r_redis.exists(r_key) and r_redis.get(r_key):
		group_name=r_redis.get(r_key).decode('utf-8')
	else:
		sql='select name from `group` where id=%s' % groupid
		res=sql_query(sql)
		if res:
			group_name=res[0][0]
			r_redis.set(r_key,group_name)

	#判断是否为群成员，非成员不能发送消息
	#读取redis
	r_key=KV_USERID_IN_GROUPID % (req_userid,groupid)
	if not r_redis.exists(r_key):
		sql='select id from group_users where groupid=%s and userid=%s' % (groupid,req_userid)
		res=sql_query(sql)
		if not res:
			send_msg(conn,"【系统提示】您非群[ %s|ID:%s ]成员，不能发送群信息！" % (group_name,groupid))
			return
		r_redis.set(r_key,"")

	#判断是否是群主，非群主判断是否群禁言,判断是否用户被禁言
	if req_userid != own_userid:
		#是否群禁言
		#读取redis
		r_key=KV_NOSPEAKING_GROUPID % groupid
		if r_redis.exists(r_key):
			is_group_forbidden_speaking=r_redis.get(r_key).decode('utf-8')
		else:
			sql='select is_group_forbidden_speaking from `group` where id=%s' % groupid
			res=sql_query(sql)
			is_group_forbidden_speaking=res[0][0]
			r_redis.set(r_key,is_group_forbidden_speaking)

		if int(is_group_forbidden_speaking):
			send_msg(conn,u'[ %s ]\n【系统消息】 群[ %s|ID:%s ]已被禁言！' % (time.strftime("%Y-%m-%d %X"),group_name,groupid))
			return 

		#是否用户被禁言
		#读取redis
		r_key=KV_NOSPEAKING_USERID_IN_GROUPID % (groupid,req_userid)
		if r_redis.exists(r_key):
			is_forbidden_speaking=r_redis.get(r_key).decode('utf-8')
		else:
			sql='select is_forbidden_speaking from group_users where groupid=%s and userid=%s' % (groupid,req_userid)
			res=sql_query(sql)
			is_forbidden_speaking=res[0][0]
			r_redis.set(r_key,is_forbidden_speaking)

		if int(is_forbidden_speaking):
			send_msg(conn,u'[ %s ]\n【系统消息】 您已被群[ %s|ID:%s ]的群主禁言！' % (time.strftime("%Y-%m-%d %X"),group_name,groupid))
			return 

	#获取群用户，发送群消息
	#清空列表
	send_time=time.strftime("%Y-%m-%d %X")
	gmsg_content='[ %s ]\n【群消息】[ %s|ID:%s ][ %s ]:%s' % (send_time,group_name,groupid,req_username,content)
	userid_list=[]
	r_key=LIST_USERIDS_OF_GROUPID % groupid
	if r_redis.llen(r_key):
		for uid in r_redis.lrange(r_key,0,-1):
			userid_list.append(int(uid.decode('utf-8')))
	else:
		sql='select userid from group_users where groupid=%s' % groupid
		res=sql_query(sql)
		for r in res:
			to_userid=r[0]
			userid_list.append(to_userid)
			#写入redis
			r_redis.rpush(r_key,to_userid)
		
	for to_userid in userid_list:
		#判断是否是本人发送
		if to_userid == req_userid:
			send_msg(conn,u'[ %s ]\n【发送群消息】[ %s|ID:%s ]:%s' % (send_time,group_name,groupid,content))
			continue
			
		#判断用户是否在线
		if user_is_online(to_userid):
			to_conn=get_userid_conn(to_userid)
			send_msg(to_conn,gmsg_content)
		else:
			r_key=LIST_GROUPMESSAGES_OF_USERID_IN_GROUPID % (groupid,to_userid)
			r_redis.rpush(r_key,gmsg_content)

def user_adduser(conn,args):
	userid=get_conn_userid(conn)	
	req_username=user_userid_name(userid)
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'【系统提示】添加好友的命令错误')
		return
	to_username=args_list[0]
	#判断用户是否存在
	sql='select id from user where username="%s"' % to_username
	res=sql_query(sql)
	if not res:
		send_msg(conn,u"【系统提示】系统不存在用户%s" % to_username)
		return
	add_userid=res[0][0]

	#判断是否添加的是自己
	if userid == add_userid:
		send_msg(conn,u"【系统提示】不能添加自己为好友！")
		return 1 

	#判断是否已经添加为好友,或者是否已经存在申请请求
	sql='select id from user_users where userid=%s and friend_userid=%s' % (userid,add_userid)
	res=sql_query(sql)
	if res:
		send_msg(conn,u"【系统提示】您和用户%s已经是好友关系，不需要再次添加" % to_username)
		return
	sql='select id from user_req where userid=%s and add_userid=%s and type=1 and status=0' % (userid,add_userid)
	res=sql_query(sql)
	if res:
		send_msg(conn,u"【系统提示】您已经向%s发送过好友申请，无需再次发送" % to_username)
		return
	
	
	to_conn=get_userid_conn(add_userid)
	sql='insert into user_req(userid,type,add_userid,status,created_at) values(%s,1,%s,0,now())' % (userid,add_userid)
	sql_dml(sql)
	if to_conn:
		sql='select id from user_req where userid=%s and add_userid=%s and status=0 LIMIT 1' %  (userid,add_userid)
		res=sql_query(sql)
		if not res:
			return
		req_id=res[0][0]
		send_msg(to_conn,u"[ %s ]【系统消息】用户%s向您申请添加好友请求!\n同意：\naccept %s \n拒绝：\nreject %s" % (time.strftime("%Y-%m-%d %X"),req_username,req_id,req_id,req_id))
	send_msg(conn,u'[ %s ]【系统消息】已向该用户%s发送添加好友申请' % (time.strftime("%Y-%m-%d %X"),to_username))

def user_deluser(conn,args):
	userid=get_conn_userid(conn)	
	req_username=user_userid_name(userid)
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'【系统提示】删除好友的命令错误')
		return
	to_username=args_list[0]
	sql='select id from user where username="%s"' % to_username
	res=sql_query(sql)
	if not res:
		send_msg(conn,u"【系统提示】系统不存在用户[ %s ]" % to_username)
		return
	del_userid=res[0][0]
	#判断是否已经是好友
	sql='select id from user_users where userid=%s and friend_userid=%s' % (userid,del_userid)
	res=sql_query(sql)
	if not res:
		send_msg(conn,u"【系统提示】您和用户[ %s ]不是好友关系" % to_username)
		return

	###开始清除redis###
	r_key=KV_USERID_ISFRIEND_USERID % (userid,del_userid)
	r_redis.delete(r_key)
	print("redis删除键值:%s",r_key)

	r_key=KV_USERID_ISFRIEND_USERID % (del_userid,userid)
	r_redis.delete(r_key)
	print("redis删除键值:%s",r_key)
	###结束清除redis###

	#双向解除好友关系
	sql='delete from user_users where userid=%s and friend_userid=%s' % (userid,del_userid) 
	sql_dml(sql)
	sql='delete from user_users where userid=%s and friend_userid=%s' % (del_userid,userid) 
	sql_dml(sql)
	send_msg(conn,u"[ %s ]【系统消息】您和用户[ %s ]已经解除好友关系" % (time.strftime("%Y-%m-%d %X"),to_username))
	
	
	#通知对方
	to_conn=get_userid_conn(del_userid)
	notice_content='[ %s ]【系统消息】用户[ %s ]已和你您解除好友关系' % (time.strftime("%Y-%m-%d %X"),req_username)
	if to_conn:
		send_msg(to_conn,notice_content)
	else:
		notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (del_userid,notice_content)
		sql_dml(notice_sql)
	

def user_entergroup(conn,args):
	req_userid=get_conn_userid(conn)
	#判断参数是否正确
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'【系统提示】加群的操作参数错误')
		return

	groupid=args_list[0]
	#判断群是否存在
	sql='select id,own_userid,name from `group` where id=%s' % groupid 
	res=sql_query(sql)
	if not res:
		send_msg(conn,"【系统提示】该群ID号[ %s ]不存在！" % groupid)
		return 
	own_userid=res[0][1]
	group_name=res[0][2]
	
	#判断用户是否已经加入
	sql='select g.id,g.name from `group` g left join group_users gu on gu.groupid=g.id where g.id=%s and (g.own_userid=%s or gu.userid=%s)' % (groupid,req_userid,req_userid)
	res=sql_query(sql)
	if res:
		send_msg(conn,u'【系统提示】您已经在群[ %s|ID:%s ]中,无需加入！'% (res[0][1],res[0][0]))
		return

	#判断是否已经存在加入该群的申请请求
	sql='select ur.id,g.id,g.name from user_req ur left join `group` g on ur.add_groupid=g.id where ur.userid=%s and ur.add_groupid=%s and ur.status=0' % (req_userid,groupid)
	res=sql_query(sql)
	if res:
		send_msg(conn,u'【系统提示】您已经申请过加入群[ %s|ID:%s ]，请等待回复！'% (res[0][2],res[0][1]))
		return
	
	#添加加群请求
	sql='insert into user_req(userid,type,add_groupid,status,created_at) values(%s,2,%s,0,now())' % (req_userid,groupid)
	sql_dml(sql)
	req_username=user_userid_name(req_userid)

	send_msg(conn,u'【系统消息】您申请了加入群[ %s|ID:%s ]，请等待回复！'% (group_name,groupid))
	
	sql='select id from user_req where userid=%s and add_groupid=%s and status=0 LIMIT 1' % (req_userid,groupid)
	res=sql_query(sql)
	if not res:
		return
	req_id=res[0][0]
	#判断群主是否在在线，在线直接发送加群提醒，如果不在线，群主通过user_req可以看到加群消息	
	if user_is_online(own_userid):
		own_conn=get_userid_conn(own_userid)
		send_msg(own_conn,u'[ %s ]【系统消息】用户[ %s ]申请加入群[ %s|ID:%s ]\n同意：\naccept %s \n拒绝：\nreject %s' % (time.strftime("%Y-%m-%d %X"),req_username,group_name,groupid,req_id,req_id,req_id))



def user_exitgroup(conn,args):
	req_userid=get_conn_userid(conn)
	req_username=user_userid_name(req_userid)
	#判断参数是否正确
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'【系统提示】退群的操作参数错误')
		return

	groupid=args_list[0]
	#判断群是否存在
	sql='select id,own_userid,name from `group` where id=%s' % groupid 
	res=sql_query(sql)
	if not res:
		send_msg(conn,"【系统提示】该群ID号[ %s ]不存在！" % groupid)
		return 
	own_userid=res[0][1]
	group_name=res[0][2]
	
	#判断用户是否已经加入
	sql='select g.id,g.name from `group` g left join group_users gu on gu.groupid=g.id where g.id=%s and (g.own_userid=%s or gu.userid=%s)' % (groupid,req_userid,req_userid)
	res=sql_query(sql)
	if not res:
		send_msg(conn,u'【系统提示】您不在群[ %s|ID:%s ]中,无需退出！'% (res[0][1],res[0][0]))
		return

	#判断是否为群主,群主无法退群
	if req_userid == own_userid:
		send_msg(conn,u'【系统提示】您是群[ %s|ID:%s ]的群主，无法退群！' % (group_name,groupid))
		return

	###开始清除redis###
	r_key=KV_USERID_IN_GROUPID % (req_userid,groupid)
	r_redis.delete(r_key)

	r_key=LIST_USERIDS_OF_GROUPID % groupid
	r_redis.delete(r_key)

	LIST_GROUPMESSAGES_OF_USERID_IN_GROUPID % (groupid,req_userid)
	r_redis.delete(r_key)
	###结束清除redis###
	#退群请求
	sql='delete from group_users where userid=%s and groupid=%s' % (req_userid,groupid)
	sql_dml(sql)
	send_msg(conn,u'【系统消息】您已退出群[ %s|ID:%s ]！' % (group_name,groupid))

	#判断群主是否在在线，在线直接发送退群提醒，如果不在线，把退群消息加入到user_notice	
	notice_content=u'[ %s ]\n【系统消息】用户[ %s ]退出群[ %s |ID号:%s] ' % (time.strftime("%Y-%m-%d %X"),req_username,group_name,groupid)
	if user_is_online(own_userid):
		own_conn=get_userid_conn(own_userid)
		send_msg(own_conn,notice_content)
	else:
		notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (own_userid,notice_content)
		sql_dml(notice_sql)
	

def user_accept(conn,args):
	cur_userid=get_conn_userid(conn)
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'【系统提示】同意请求的操作参数错误')
		return	
	req_id=args_list[0]
	sql='select id,userid,add_userid,add_groupid from user_req where id=%s and status=0' % req_id
	res=sql_query(sql)
	if not res:
		send_msg(conn,u'【系统提示】请求的ID号不存在')
		return
	req_userid=res[0][1]
	add_userid=res[0][2]
	add_groupid=res[0][3]
	#判断是加群还是添加好友 
	#加好友
	if add_userid:
		#判断当前用户是否有权限执行
		if cur_userid != add_userid:
			send_msg(conn,u'【系统提示】您无权执行该操作！')
			return 1
			
		#互加好友操作
		sql='insert into user_users(userid,friend_userid,created_at) values(%s,%s,now())' % (req_userid,add_userid)
		sql_dml(sql)
		sql='insert into user_users(userid,friend_userid,created_at) values(%s,%s,now())' % (add_userid,req_userid)
		sql_dml(sql)

		#消息提醒内容
		notice_content=u'[ %s ]\n【系统消息】用户%s已经同意添加您为好友!' % (time.strftime("%Y-%m-%d %X"),user_userid_name(add_userid))
		send_msg(conn,u'[ %s ]\n【系统消息】您已同意添加%s为好友' % user_userid_name(add_userid))

	#加群
	if add_groupid:
		#判断当前用户是否是群主,非群主不能执行该操作
		sql='select own_userid from `group` where id=%s' % add_groupid
		res=sql_query(sql)
		if not res:
			send_msg(conn,u'【系统提示】遇到UF0！')
			return
		own_userid=res[0][0]
		if cur_userid != own_userid:
			send_msg(conn,u'【系统提示】您非该群的群主，无权执行该操作！')
			return 2 
		
		#把用户加入群
		sql='insert into group_users(userid,groupid,created_at) values(%s,%s,now())' % (req_userid,add_groupid)
		sql_dml(sql)


		#获取群的信息
		sql='select id,name,own_userid from `group` where id=%s' % add_groupid
		res=sql_query(sql)
		if res:
			groupid=res[0][0]
			group_name=res[0][1]
			group_own_userid=res[0][2]	
			#消息提醒内容
			notice_content=u"[ %s ]\n【系统消息】[群主:%s]已经同意你加入群[ %s|ID:%s ]！" % (time.strftime("%Y-%m-%d %X"),user_userid_name(group_own_userid),group_name,groupid)
			send_msg(conn,u'[ %s ]\n【系统消息】您已同意用户[ %s ]加入群[ %s|ID:%s ]！' %(user_userid_name(req_userid),group_name,groupid) )

	#判断该请求的用户是否在线，如果在线直接提醒用户，不在线插入提醒信息到数据库
	if user_is_online(req_userid):
		req_conn=get_userid_conn(req_userid)
		send_msg(req_conn,notice_content)
	else:
		notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (req_userid,notice_content)
		sql_dml(notice_sql)

	#更新请求状态
	sql='update user_req set status=1 where id=%s' % req_id
	sql_dml(sql)

def user_reject(conn,args):
	cur_userid=get_conn_userid(conn)
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'【系统提示】拒绝请求的操作参数错误')
		return	
	req_id=args_list[0]
	sql='select id,userid,add_userid,add_groupid from user_req where id=%s and status=0' % req_id
	res=sql_query(sql)
	if not res:
		send_msg(conn,u'【系统提示】请求的ID号不存在')
		return
	req_userid=res[0][1]
	add_userid=res[0][2]
	add_groupid=res[0][3]

	#如果是添加好友 
	if add_userid:
		#判断当前用户是否有权限执行
		if cur_userid != add_userid:
			send_msg(conn,u'【系统提示】您无权执行该操作！')
			return 1
		notice_content=u'[ %s ]\n【系统消息】用户%s已经婉拒了您添加好友的请求！' % (time.strftime("%Y-%m-%d %X"),user_userid_name(add_userid))
		send_msg(conn,u'[ %s ]\n【系统消息】您已拒绝用户%s的添加好友申请' % user_userid_name(add_userid))

	#如果是添加群
	if add_groupid:
		#判断当前用户是否是群主,非群主不能执行该操作
		sql='select own_userid from `group` where id=%s' % add_groupid
		res=sql_query(sql)
		if not res:
			send_msg(conn,u'【系统提示】遇到UF0！')
			return
		own_userid=res[0][0]
		if cur_userid != own_userid:
			send_msg(conn,u'【系统提示】您非该群的群主，无权执行该操作！')
			return 2 
		#获取群的信息
		sql='select id,name,own_userid from `group` where id=%s' % add_groupid
		res=sql_query(sql)
		if res:
			groupid=res[0][0]
			group_name=res[0][1]
			group_own_userid=res[0][2]	
			notice_content=u"[ %s ]\n【系统消息】[群主:%s]已经拒绝你加入群[ %s|ID:%s ]的请求！" % (time.strftime("%Y-%m-%d %X"),user_userid_name(group_own_userid),group_name,groupid)
			send_msg(conn,u'[ %s ]【系统提示】您已拒绝用户[ %s ]加入群[ %s|ID:%s ]的请求！' % (time.strftime("%Y-%m-%d %X"),user_userid_name(req_userid),group_name,groupid) )

	#判断用户是否在线，如果在线直接提醒用户，不在线插入提醒信息到数据库
	if user_is_online(req_userid):
		req_conn=get_userid_conn(req_userid)
		send_msg(req_conn,notice_content)
	else:
		notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (req_userid,notice_content)
		sql_dml(notice_sql)


	#更新请求状态
	sql='update user_req set status=1 where id=%s' % req_id
	sql_dml(sql)
		

def user_listfriends(conn,args):
	userid=get_conn_userid(conn)
	sql='select u.id,u.username from user_users uu join user u on uu.friend_userid=u.id  where uu.userid=%s' % userid
	res=sql_query(sql)
	if not res:
		 firends_info_msg=u'当前没有添加好友'
	else:
		friends=[]	
		for r in res:
			friend_userid=r[0]
			friend_username=r[1]
			friend_info="- "+friend_username
			#判断是否在线
			if user_is_online(friend_userid):
				friend_info+='(在线)'
			else:
				friend_info+='(离线)'
			friends.append(friend_info)
		firends_info_msg=u'当前好友:\n%s' % '\n'.join(friends)
	send_msg(conn,firends_info_msg)

def user_listgroup(conn,args):
	own_userid=get_conn_userid(conn)
	args_list=args
	if len(args_list)!=0:
		send_msg(conn,u'【系统提示】查看群的操作参数错误')
		return	
	#查看当前用户拥有的群
	own_groups=[]
	sql='select id,name from `group` where own_userid=%s' % own_userid 
	res=sql_query(sql)
	if not res:
		own_groups.append('- 无')	
	else:
		for r in res:
			own_groups.append('- 群名：'+r[1]+'|群ID号：'+str(r[0]))

	#查看当前用户加入的群
	add_groups=[]
	sql='select g.id,g.name from `group_users` gu join `group` g on gu.groupid=g.id where g.own_userid!=%s and gu.userid=%s' % (own_userid,own_userid)
	res=sql_query(sql)
	if not res:
		add_groups.append('- 无')
	else:
		for r in res:
			add_groups.append('- 群名：'+r[1]+'|群ID号：'+str(r[0]))
	send_msg(conn,"您拥有的群:\n%s\n您加入的群:\n%s\n" % ('\n'.join(own_groups),'\n'.join(add_groups)))

def user_listallgroup(conn,args):
	own_userid=get_conn_userid(conn)
	args_list=args
	if len(args_list)!=0:
		send_msg(conn,u'【系统提示】查看群的操作参数错误')
		return	
	#查看当前用户拥有的群
	groups=[]
	sql='select id,name from `group`' 
	res=sql_query(sql)
	if not res:
		own_groups.append('- 无')	
	else:
		for r in res:
			#判断当前用户是否为群主
			groupid=r[0]
			group_name=r[1]
			sql='select id from `group` where id=%s and own_userid=%s' % (groupid,own_userid)
			res=sql_query(sql)
			if res:
				groups.append('- 群名：'+group_name+'|群ID号：'+str(groupid)+'(您是群主)')
			else:
				#判断当前用户是否在群内
				sql='select id from group_users where groupid=%s and userid=%s' % (groupid,own_userid)
				res=sql_query(sql)
				if res:
					groups.append('- 群名：'+group_name+'|群ID号：'+str(groupid)+'(您已加入)')
				else:
					groups.append('- 群名：'+group_name+'|群ID号：'+str(groupid))

	send_msg(conn,"当前系统的群:\n%s" % '\n'.join(groups))

def user_listgroupusers(conn,args):
	req_userid=get_conn_userid(conn)
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'【系统提示】列出群成员的操作参数错误')
		return	
	groupid=args_list[0]

	#判断群是否存在
	sql='select id,own_userid,name from `group` where id=%s' % groupid 
	res=sql_query(sql)
	if not res:
		send_msg(conn,"【系统提醒】该群ID号[ %s ]不存在！" % groupid)
		return 
	own_userid=res[0][1]
	group_name=res[0][2]

	#判断是否为群成员，非成员不能查看群信息
	sql='select id from group_users where groupid=%s and userid=%s' % (groupid,req_userid)
	res=sql_query(sql)
	if not res:
		send_msg(conn,"【系统提示】您非群[ %s|ID:%s ]成员，不能查看群成员信息！" % (group_name,groupid))
		return
	#查看群成员信息
	sql='select userid from group_users where groupid=%s' % groupid
	res=sql_query(sql)
	mem_list=[]
	if res:
		for r in res:
			mem_info=""
			mem_userid=r[0]
			mem_user_name=user_userid_name(mem_userid)
			mem_info='- '+mem_user_name
			#判断是否是群主
			if mem_userid == own_userid:
				mem_info+='[群主]'	
			else:
				mem_info+='[成员]'	

			#判断是否在线
			if user_is_online(mem_userid):
				mem_info+='(在线)'
			else:
				mem_info+='(离线)'
			mem_list.append(mem_info)
		send_msg(conn,'群[ %s|ID:%s ]成员信息\n%s' %(group_name,groupid,'\n'.join(mem_list)))
		

def user_creategroup(conn,args):
	own_userid=get_conn_userid(conn)
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'【系统提示】创建群的操作参数错误')
		return	
	group_name=args_list[0]

	#判断该用户的群是否已经达到上限
	sql='select groups_limit from user where id=%s' % own_userid
	res=sql_query(sql)
	if not res:
		send_msg(conn,u'【系统提示】查看数据库异常')
		return 
	groups_limit=res[0][0]
	if groups_limit == 0:
		send_msg(conn,u'【系统提示】您创建的群数量已达上限数为%s' % groups_limit)
		return
	sql='select count(1) from `group` where own_userid=%s' % own_userid
	res=sql_query(sql)
	if not res:
		send_msg(conn,u'【系统提示】查看数据库异常')
		return
	group_count=res[0][0]
	if group_count+1>groups_limit:
		send_msg(conn,u'【系统提示】您创建的群数量已达上限数为%s,当前拥有群数为%s' % (groups_limit,group_count))
		return

	#建群
	sql='insert into `group`(name,own_userid,is_empty,created_at) values("%s",%s,0,now())' % (group_name,own_userid)
	sql_dml(sql)

	#把当前用户添加到群内

	#获取刚创立的群id
	sql='select id from `group` where own_userid=%s and is_empty=0' % own_userid;
	res=sql_query(sql)
	if res:
		groupid=res[0][0]
		#把当前用户加到群内
		sql='insert into `group_users`(userid,groupid,created_at) values(%s,%s,now())' % (own_userid,groupid)
		sql_dml(sql)
		#添加完成员把is_empty设置为1
		sql='update `group` set is_empty=1 where id=%s' % groupid
		sql_dml(sql)
	
	send_msg(conn,u'[ %s ]【系统消息】群[ %s ]已经创建完成！' % (time.strftime("%Y-%m-%d %X"),group_name))

def user_kickout(conn,args):
	req_userid=get_conn_userid(conn)
	args_list=args
	if len(args_list)!=2:
		send_msg(conn,u'【系统提示】操作参数错误')
		return	
	groupid=args_list[0]
	to_username=args_list[1]
	#判断群是否存在
	sql='select own_userid,name from `group` where id=%s' % groupid 
	res=sql_query(sql)
	if not res:
		send_msg(conn,"【系统提示】群ID号[ %s ]不存在！" % groupid)
		return 
	own_userid=res[0][0]
	own_username=user_userid_name(own_userid)
	groupname=res[0][1]

	#判断是否为群主,非群主不能踢用户
	if req_userid != own_userid:
		send_msg(conn,u'[ %s ]\n【系统消息】 您不是群[ %s|ID:%s ]的群主，无权移出用户！' % (time.strftime("%Y-%m-%d %X"),group_name,groupid))
		return

	#判断用户是否存在
	sql='select id from user where username="%s"' % to_username
	res=sql_query(sql)
	if not res:
		send_msg(conn,u"【系统提示】用户[ %s ]不存在！" % to_username)
		return
	to_userid=res[0][0]

	#判断当前用户是否在群内
	sql='select id from group_users where groupid=%s and userid=%s' % (groupid,to_userid)
	res=sql_query(sql)
	if not res:
		send_msg(conn,u"【系统提示】用户[ %s ]不在群[ %s|ID: ]内！" % (to_username,groupname,groupid))
		return

	###开始清理redis###
	r_key=KV_USERID_IN_GROUPID % (to_userid,groupid)
	r_redis.delete(r_key)

	r_key=LIST_USERIDS_OF_GROUPID % groupid
	r_redis.delete(r_key)
	###结束清理redis###

	#提醒群用户
	sql='select userid from group_users where groupid=%s' % groupid
	res=sql_query(sql)
	for r in res:
		notice_userid=r[0]
		#判断是否被踢用户
		if notice_userid == to_userid:
			notice_content='[ %s ]\n【群消息】您已被群主[ %s ]移出群[ %s|ID:%s ]！' % (time.strftime("%Y-%m-%d %X"),own_username,groupname,groupid)
		else:
			notice_content='[ %s ]\n【群消息】用户[ %s ]已被群主[ %s ]移出群[ %s|ID:%s ]！' % (time.strftime("%Y-%m-%d %X"),to_username,own_username,groupname,groupid)
		#判断用户是否在线
		if user_is_online(notice_userid):
			notice_conn=get_userid_conn(notice_userid)
			send_msg(notice_conn,notice_content)
		else:
			notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (notice_userid,notice_content)
			sql_dml(notice_sql)

	#踢出用户
	sql='delete from group_users where groupid=%s and userid=%s' % (groupid,to_userid)
	sql_dml(sql)

def user_delgroup(conn,args):
	req_userid=get_conn_userid(conn)
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'【系统提醒】删群的操作参数错误')
		return	
	groupid=args_list[0]

	#判断群是否存在
	sql='select id,own_userid,name from `group` where id=%s' % groupid 
	res=sql_query(sql)
	if not res:
		send_msg(conn,"【系统提醒】该群ID号[ %s ]不存在！" % groupid)
		return 
	own_userid=res[0][1]
	group_name=res[0][2]

	#判断是否为群主,非群主不能删群
	if req_userid != own_userid:
		send_msg(conn,u'[ %s ]\n【系统提示】 您不是群[ %s|ID:%s ]的群主，无法删群！' % (time.strftime("%Y-%m-%d %X"),group_name,groupid))
		return

	#获取群用户，发送删群提示
	sql='select userid from group_users where groupid=%s and userid!=%s' % (groupid,own_userid)
	res=sql_query(sql)
	if res:
		for r in res:
			to_userid=r[0]
			notice_content='[ %s ]\n【系统消息】 群主已解散群[ %s|ID:%s ]！' % (time.strftime("%Y-%m-%d %X"),group_name,groupid)
			#判断用户是否在线
			if user_is_online(to_userid):
				to_conn=get_userid_conn(to_userid)
				send_msg(to_conn,notice_content)
			else:
				notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (to_userid,notice_content)
				sql_dml(notice_sql)
	
	###开始更新redis###
	r_key=KV_EXISTS_GROUPID % groupid
	r_redis.delete(r_key)

	r_key=KV_GROUPID_GET_GROUPNAME % groupid
	r_redis.delete(r_key)

	r_key=KV_GROUPID_GET_OWN_USERID % groupid
	r_redis.delete(r_key)

	r_key=KV_OWNUSERID_OWN_GROUPID % (own_userid,groupid)
	r_redis.delete(r_key)

	r_key=LIST_USERIDS_OF_GROUPID % groupid
	r_redis.delete(r_key)

	sql='select userid from group_users where groupid=%s' % groupid
	res=sql_query(sql)
	for r in res:
		uid=r[0]
		r_key=KV_USERID_IN_GROUPID % (uid,groupid)
		r_redis.delete(r_key)

		r_key=LIST_GROUPMESSAGES_OF_USERID_IN_GROUPID % (groupid,uid)
		r_redis.delete(r_key)

	###结束更新redis###
			
	#删除群用户
	sql='delete from group_users where groupid=%s ' % groupid
	sql_dml(sql)

	#删群
	sql='delete from `group` where id=%s ' % groupid 
	sql_dml(sql)
	
	#更新redis

	send_msg(conn,'[ %s ]\n【系统消息】 群[ %s|ID:%s ]已解散！' % (time.strftime("%Y-%m-%d %X"),group_name,groupid))

def user_nospkuser(conn,args):
	req_userid=get_conn_userid(conn)
	args_list=args
	if len(args_list)!=2:
		send_msg(conn,u'【系统提示】禁言用户操作参数错误！')
		return	
	groupid=args_list[0]
	to_username=args_list[1]
	#判断群是否存在
	sql='select own_userid,name from `group` where id=%s' % groupid 
	res=sql_query(sql)
	if not res:
		send_msg(conn,"【系统提示】群ID号[ %s ]不存在！" % groupid)
		return 
	own_userid=res[0][0]
	own_username=user_userid_name(own_userid)
	groupname=res[0][1]

	#判断是否为群主,非群主不能踢用户
	if req_userid != own_userid:
		send_msg(conn,u'[ %s ]\n【系统消息】 您不是群[ %s|ID:%s ]的群主，无权禁言用户！' % (groupname,groupid))
		return

	#判断用户是否存在
	sql='select id from user where username="%s"' % to_username
	res=sql_query(sql)
	if not res:
		send_msg(conn,u"【系统提示】用户[ %s ]不存在！" % to_username)
		return
	to_userid=res[0][0]

	#判断当前用户是否在群内,并且是否禁用
	sql='select id,is_forbidden_speaking from group_users where groupid=%s and userid=%s' % (groupid,to_userid)
	res=sql_query(sql)
	if not res:
		send_msg(conn,u"【系统提示】用户[ %s ]不在群[ %s|ID: ]内！" % (to_username,groupname,groupid))
		return
	else:
		is_forbidden_speaking=res[0][1]
		if is_forbidden_speaking:
			send_msg(conn,u"【系统消息】用户 [ %s ]在群[ %s|ID:%s ]内已被禁言，无需再次操作！" % (to_username,groupname,groupid))
			return

	###开始清理redis###
	r_key=KV_NOSPEAKING_USERID_IN_GROUPID % (groupid,to_userid)
	r_redis.delete(r_key)
	###结束清理redis###

	#提醒群用户
	sql='select userid from group_users where groupid=%s' % groupid
	res=sql_query(sql)
	for r in res:
		notice_userid=r[0]
		#判断是否被踢用户
		if notice_userid == to_userid:
			notice_content='[ %s ]\n【系统消息】您已被群[ %s|ID:%s ]的群主[ %s ]禁言！' % (time.strftime("%Y-%m-%d %X"),groupname,groupid,own_username)
		else:
			notice_content='[ %s ]\n【系统消息】用户[ %s ]已被群[ %s|ID:%s ]的群主[ %s ]禁言！' % (time.strftime("%Y-%m-%d %X"),to_username,groupname,groupid,own_username)
		#判断用户是否在线
		if user_is_online(notice_userid):
			notice_conn=get_userid_conn(notice_userid)
			send_msg(notice_conn,notice_content)
		else:
			notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (notice_userid,notice_content)
			sql_dml(notice_sql)

	#禁言用户
	sql='update group_users set is_forbidden_speaking=1 where groupid=%s and userid=%s' % (groupid,to_userid)
	sql_dml(sql)

def user_spkuser(conn,args):
	req_userid=get_conn_userid(conn)
	args_list=args
	if len(args_list)!=2:
		send_msg(conn,u'【系统提示】取消禁言用户操作参数错误！')
		return	
	groupid=args_list[0]
	to_username=args_list[1]
	#判断群是否存在
	sql='select own_userid,name from `group` where id=%s' % groupid 
	res=sql_query(sql)
	if not res:
		send_msg(conn,"【系统提示】群ID号[ %s ]不存在！" % groupid)
		return 
	own_userid=res[0][0]
	own_username=user_userid_name(own_userid)
	groupname=res[0][1]

	#判断是否为群主,非群主不能操作
	if req_userid != own_userid:
		send_msg(conn,u'[ %s ]\n【系统消息】 您不是群[ %s|ID:%s ]的群主，无权取消禁言用户！' % (groupname,groupid))
		return

	#判断用户是否存在
	sql='select id from user where username="%s"' % to_username
	res=sql_query(sql)
	if not res:
		send_msg(conn,u"【系统提示】用户[ %s ]不存在！" % to_username)
		return
	to_userid=res[0][0]

	#判断当前用户是否在群内,并且是否禁用
	sql='select id,is_forbidden_speaking from group_users where groupid=%s and userid=%s' % (groupid,to_userid)
	res=sql_query(sql)
	if not res:
		send_msg(conn,u"【系统提示】用户[ %s ]不在群[ %s|ID: ]内！" % (to_username,groupname,groupid))
		return
	else:
		is_forbidden_speaking=res[0][1]
		if not is_forbidden_speaking:
			send_msg(conn,u"【系统消息】用户 [ %s ]在群[ %s|ID:%s ]内未被禁言，无需此操作！" % (to_username,groupname,groupid))
			return

	###开始清理redis###
	r_key=KV_NOSPEAKING_USERID_IN_GROUPID % (groupid,to_userid)
	r_redis.delete(r_key)
	###结束清理redis###

	#提醒群用户
	sql='select userid from group_users where groupid=%s' % groupid
	res=sql_query(sql)
	for r in res:
		notice_userid=r[0]
		#判断是否被踢用户
		if notice_userid == to_userid:
			notice_content='[ %s ]\n【系统消息】您已被群[ %s|ID:%s ]的群主[ %s ]取消禁言！' % (time.strftime("%Y-%m-%d %X"),groupname,groupid,own_username)
		else:
			notice_content='[ %s ]\n【系统消息】用户[ %s ]已被群[ %s|ID:%s ]的群主[ %s ]取消禁言！' % (time.strftime("%Y-%m-%d %X"),to_username,groupname,groupid,own_username)
		#判断用户是否在线
		if user_is_online(notice_userid):
			notice_conn=get_userid_conn(notice_userid)
			send_msg(notice_conn,notice_content)
		else:
			notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (notice_userid,notice_content)
			sql_dml(notice_sql)

	#取消禁言用户
	sql='update group_users set is_forbidden_speaking=0 where groupid=%s and userid=%s' % (groupid,to_userid)
	sql_dml(sql)

def user_nospkgroup(conn,args):
	req_userid=get_conn_userid(conn)
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'【系统提示】禁言群操作参数错误！')
		return	
	groupid=args_list[0]

	#判断群是否存在
	sql='select own_userid,name,is_group_forbidden_speaking from `group` where id=%s' % groupid 
	res=sql_query(sql)
	if not res:
		send_msg(conn,"【系统提示】群ID号[ %s ]不存在！" % groupid)
		return 

	own_userid=res[0][0]
	own_username=user_userid_name(own_userid)
	groupname=res[0][1]
	is_group_forbidden_speaking=res[0][2]

	#判断是否为群主,非群主不能操作
	if req_userid != own_userid:
		send_msg(conn,u'【系统消息】您不是群[ %s|ID:%s ]的群主，无权禁言群！' % (groupname,groupid))
		return

	#判断当前群是否被禁言
	if is_group_forbidden_speaking:
		send_msg(conn,u"【系统消息】群[ %s|ID:%s ]内已被禁言，无需再次操作！" % (groupname,groupid))
		return

	###开始清理redis###
	r_key=KV_NOSPEAKING_GROUPID % groupid
	r_redis.delete(r_key)
	###结束清理redis###

	#提醒群用户
	sql='select userid from group_users where groupid=%s' % groupid
	res=sql_query(sql)
	for r in res:
		notice_userid=r[0]
		notice_content='[ %s ]\n【系统消息】群[ %s|ID:%s ]已被群主[ %s ]禁言！' % (time.strftime("%Y-%m-%d %X"),groupname,groupid,own_username)
		#判断用户是否在线
		if user_is_online(notice_userid):
			notice_conn=get_userid_conn(notice_userid)
			send_msg(notice_conn,notice_content)
		else:
			notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (notice_userid,notice_content)
			sql_dml(notice_sql)

	#群禁言
	sql='update `group` set is_group_forbidden_speaking=1 where id=%s' % groupid
	sql_dml(sql)

def user_spkgroup(conn,args):
	req_userid=get_conn_userid(conn)
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'【系统提示】取消禁言群操作参数错误！')
		return	
	groupid=args_list[0]

	#判断群是否存在
	sql='select own_userid,name,is_group_forbidden_speaking from `group` where id=%s' % groupid 
	res=sql_query(sql)
	if not res:
		send_msg(conn,"【系统提示】群ID号[ %s ]不存在！" % groupid)
		return 

	own_userid=res[0][0]
	own_username=user_userid_name(own_userid)
	groupname=res[0][1]
	is_group_forbidden_speaking=res[0][2]

	#判断是否为群主,非群主不能操作
	if req_userid != own_userid:
		send_msg(conn,u'【系统消息】 您不是群[ %s|ID:%s ]的群主，无权取消群禁言！' % (groupname,groupid))
		return

	#判断当前群是否被禁言
	if not is_group_forbidden_speaking:
		send_msg(conn,u"【系统消息】群[ %s|ID:%s ]内未被禁言，无需此操作！" % (groupname,groupid))
		return

	###开始清理redis###
	r_key=KV_NOSPEAKING_GROUPID % groupid
	r_redis.delete(r_key)
	###结束清理redis###

	#提醒群用户
	sql='select userid from group_users where groupid=%s' % groupid
	res=sql_query(sql)
	for r in res:
		notice_userid=r[0]
		notice_content='[ %s ]\n【系统消息】群[ %s|ID:%s ]已被群主[ %s ]取消禁言！' % (time.strftime("%Y-%m-%d %X"),groupname,groupid,own_username)
		#判断用户是否在线
		if user_is_online(notice_userid):
			notice_conn=get_userid_conn(notice_userid)
			send_msg(notice_conn,notice_content)
		else:
			notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (notice_userid,notice_content)
			sql_dml(notice_sql)

	#取消群禁言
	sql='update `group` set is_group_forbidden_speaking=0 where id=%s' % groupid
	sql_dml(sql)

def close_clear_all(conn,fd):
	print("清理客户端中...")
	try:
		epoll.unregister(fd)
		#从命令统计列表删除
		del fd_to_command_count[fd]
		#从在线userid2sock删除
		del_online_sock(conn)
		#从queue队列中删除
		del message_queue[fd]
		#从在线fd2sock字典删除
		if fd in auth_fd_to_socket.keys():
			del auth_fd_to_socket[fd] 
		fd_to_socket[fd].close()
		del fd_to_socket[fd]
	except:
		print("清理客户端发生异常")	
	print("清理客户端完成.")


def handle_epollin(fd,c_sock):
	#判断用户是否登录,如果没登录就转到EPOLLOUT
	c_sock=fd_to_socket[fd]
	if fd not in auth_fd_to_socket.keys():
		#获取数据
		try:
			length=struct.unpack('H',c_sock.recv(2))[0]
			data=c_sock.recv(int(length))
		except:
			print(u'[ %s ] 获取客户端数据异常' % time.strftime("%Y-%m-%d %X"))
			err_msg=u'echo 数据包异常'
			message_queue[fd].put(err_msg)
			#获取客户端数据异常，猜测客户端非正常关闭
			epoll.modify(fd,0)
			try:
				fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
			except:
				close_clear_all(c_sock,fd)
				
			return	
		#判断包头字段里填入的长度和实际收到的是否一致	
		if len(data)!=length:
			print(u'[ %s ] 客户端数据包长度异常' % time.strftime("%Y-%m-%d %X"))
			err_msg=u'echo 数据包长度异常'
			message_queue[fd].put(err_msg)
			return

		#判断命令是否为auth,是，则进行认证，否则跳转到EPOLLOUT
		else:
			cmd=data.decode('utf-8').split(' ')[0]
			#进行认证
			if cmd == 'auth':
				#获取用户名和密码
				try:
					user=data.decode('utf-8').split(' ')[1]
					pwd=data.decode('utf-8').split(' ')[2]
				except:
					try:
						epoll.modify(fd,select.EPOLLOUT)	
					except:
						epoll.modify(fd,0)
						fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
					return

				#获取用户userid
				userid=user_name_userid(user)

				if userid < 0:
					login_err_msg=u'echo 用户名不存在！'
					message_queue[fd].put(login_err_msg)
					try:
						epoll.modify(fd,select.EPOLLOUT)	
					except:
						epoll.modify(fd,0)
						fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
					return
			
				#进行认证
				if user_auth(user,pwd):
					#注销同一账号其他客户端的连接,实现单点登录
					try:
						#获取用户id
						userid=user_name_userid(user)	
						#获取用户socket
						user_conn=get_userid_conn(userid)
						#对用户发送sock请求
						logout_msg=u'echo 该账号在其他地方登录，您被迫已下线！'
						message_queue[user_conn.fileno()].put(logout_msg)
						try:
							epoll.modify(user_conn.fileno(),select.EPOLLOUT)	
							#清除认证用户列表
							del_online_sock(user_conn)
							#清除登录列表的记录
							if user_conn.fileno() in auth_fd_to_socket.keys():
								del auth_fd_to_socket[user_conn.fileno()]
						except:
							epoll.modify(user_conn.fileno(),0)
							fd_to_socket[user_conn.fileno()].shutdown(socket.SHUT_RDWR)
					except:
						pass
					#推送消息
					#push_new_msg(c_sock,user)
					#添加到在线用户列表
					userid_to_socket[userid]=c_sock
					#添加到在线fd2sock列表
					auth_fd_to_socket[fd]=c_sock
					#提示登录成功的信息
					#加入用户sock2command_count
					fd_to_command_count[fd]=0
					login_ok=u'echo 登录成功!'
					message_queue[fd].put(login_ok)
					try:
						print("[ %s ]开始修改epollout!" % time.ctime())
						epoll.modify(fd,select.EPOLLOUT)	
					except:
						epoll.modify(fd,0)
						fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
					return
				else:
					login_fail=u'echo 登录失败，密码错误！'
					message_queue[fd].put(login_fail)
					try:
						epoll.modify(fd,select.EPOLLOUT)	
					except:
						epoll.modify(fd,0)
						fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
					return
			#跳转到EPOLLOUT
			else:
				try:
					epoll.modify(fd,select.EPOLLOUT)	
				except:
					epoll.modify(fd,0)
					fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
				return

	#如果已登录，则提取数据，放入到Quene中，跳转到EPOLLOUT
	else:
		#获取数据
		try:
			length=struct.unpack('H',c_sock.recv(2))[0]
			data=c_sock.recv(int(length))
		except:
			print(u'[ %s ] 获取客户端数据异常' % time.strftime("%Y-%m-%d %X"))
			err_msg=u'echo 数据包异常'
			message_queue[fd].put(err_msg)
			epoll.modify(fd,0)
			fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
			return
		#判断包头字段里填入的长度和实际收到的是否一致	
		if len(data)!=length:
			print(u'[ %s ] 客户端数据包长度异常' % time.strftime("%Y-%m-%d %X"))
			err_msg=u'echo 数据包异常'
			message_queue[fd].put(err_msg)
			try:
				epoll.modify(fd,select.EPOLLHUP)
			except:
				epoll.modify(fd,0)
				fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
			return
		
		#把数据存入队列
		message_queue[fd].put(data.decode('utf-8'))
		try:
			epoll.modify(fd,select.EPOLLOUT)	
		except:
			epoll.modify(fd,0)
			fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
	
def handle_epollout(fd,c_sock):
	#判断用户是否登录,如果没登录就推送登录提醒消息
	c_sock=fd_to_socket[fd]
	if fd not in auth_fd_to_socket.keys():
		#获取队列中的内容
		try:
			message=message_queue[fd].get_nowait()
			cmd=message.split(' ')[0]
			#判断是否是服务器的信息
			if cmd == 'echo':
				echo_reply=message.split(' ')[1:]
				send_msg(c_sock,' '.join(echo_reply))
			elif cmd == 'close':
				print(u'[ %s ] 客户端和服务器断开连接' % time.strftime("%Y-%m-%d %X"))
				send_msg(c_sock,u'您已经与服务器断开连接')
				#关闭连接
				epoll.modify(fd,0)
				fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
				return
		except queue.Empty:
			prompt_auth_msg=u'请输入用户名密码登录！！！\n命令格式:auth 用户名 密码'
			send_msg(c_sock,prompt_auth_msg)
		try:
			epoll.modify(fd,select.EPOLLIN)
		except:
			epoll.modify(fd,0)
			fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
		return
	#如果已经登录则推送，获取队列中的内容，判断消息的类别，调用不同的函数处理
	else:
		#推送消息
		if fd_to_command_count[fd]==0:
			push_userid=get_conn_userid(c_sock)
			if push_userid:
				push_new_msg(c_sock,push_userid)
		fd_to_command_count[fd]+=1
		#获取队列中的内容
		try:
			message=message_queue[fd].get_nowait()
		except queue.Empty:
			try:
				epoll.modify(fd,select.EPOLLIN)
			except:
				epoll.modify(fd,0)
				fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
			return
		cmd=message.split(' ')[0]
		#判断是否是服务器的信息
		#系统发出的消息
		if cmd == 'echo':
			echo_reply=message.split(' ')[1:]
			send_msg(c_sock,' '.join(echo_reply))
		elif cmd == 'help' or cmd == '?':
			send_msg(c_sock,cmd_help)
		elif cmd == 'close':
			print(u'[ %s ] 客户端和服务器断开连接' % time.strftime("%Y-%m-%d %X"))
			send_msg(c_sock,u'您已经与服务器断开连接')
			#关闭连接
			epoll.modify(fd,0)
			fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
			return
		elif cmd == 'logout':
			print(u'[ %s ] 退出系统' % time.strftime("%Y-%m-%d %X"))
			send_msg(c_sock,u'退出系统！')
			#清除认证用户列表
			del_online_sock(c_sock)
			#重置执行命令的列表
			fd_to_command_count[fd]=0
			#清除登录列表的记录
			if fd in auth_fd_to_socket.keys():
				del auth_fd_to_socket[fd]
		elif cmd == 'auth':
			send_msg(c_sock,u'【系统提示】提示!您已认证！')
		elif cmd not in noargs_cmd+args_cmd:
			print(u'[ %s ] 客户端的命令不支持,请使用 help/?' % time.strftime("%Y-%m-%d %X"))
			send_msg(c_sock,u'【系统提示】输入的命令不支持,请使用 help/?')
		else:
			args=message.split(' ')[1:]
			#命令分发
			switch={
				'msg': user_msg,
				'm': user_msg,
				'gmsg': user_gmsg,
				'gm': user_gmsg,
				'adduser': user_adduser,
				'au': user_adduser,
				'deluser': user_deluser,
				'du': user_deluser,
				'entergroup': user_entergroup,
				'eng': user_entergroup,
				'exitgroup': user_exitgroup,
				'exg': user_exitgroup,
				'kickout': user_kickout,
				'ko': user_kickout,
				'reject': user_reject,
				'reject': user_reject,
				'rj': user_reject,
				'accept': user_accept,
				'ac': user_accept,
				'listfriends': user_listfriends,
				'lf': user_listfriends,
				'listgroup': user_listgroup,
				'lg': user_listgroup,
				'listallgroup': user_listallgroup,
				'lag': user_listallgroup,
				'listgroupusers': user_listgroupusers,
				'lgu': user_listgroupusers,
				'creategroup': user_creategroup,
				'cg': user_creategroup,
				'delgroup': user_delgroup,
				'dg': user_delgroup,
				'nospkgroup': user_nospkgroup,
				'nsg': user_nospkgroup,
				'spkgroup': user_spkgroup,
				'sg': user_spkgroup,
				'nospkuser': user_nospkuser,
				'nsu': user_nospkuser,
				'spkuser': user_spkuser,
				'su': user_spkuser
			}
			switch[cmd](c_sock,args)
		try:
			epoll.modify(fd,select.EPOLLIN)
		except:
			epoll.modify(fd,0)

def load_data2redis(): 
	#用户好友关系,查询好友关系表
	sql='select userid,friend_userid from user_users'
	res=sql_query(sql)
	if res:
		for r in res:
			userid=r[0]
			friend_userid=r[1]
			r_key=KV_USERID_ISFRIEND_USERID % (userid,friend_userid)
			r_redis.set(r_key,'')

	#查询群包含的群成员
	#清空lieb
	sql='select id from `group`'
	res=sql_query(sql)
	for r in res:
		groupid=r[0]
		r_key=LIST_USERIDS_OF_GROUPID % groupid
		r_redis.ltrim(r_key,1,0)

	sql='select userid,groupid,is_forbidden_speaking from group_users'
	res=sql_query(sql)
	for r in res:
		userid=int(r[0])
		groupid=r[1]	
		is_forbidden_speaking=int(r[2])
		r_key=LIST_USERIDS_OF_GROUPID % groupid
		r_redis.rpush(r_key,userid)

		r_key=KV_NOSPEAKING_USERID_IN_GROUPID % (groupid,userid)
		r_redis.set(r_key,is_forbidden_speaking)

	#查询所有群ID,是否禁用，所有群成员ID列表
	sql='select id,is_group_forbidden_speaking from `group`'
	res=sql_query(sql)
	for r in res:
		gid=r[0]
		is_group_forbidden_speaking=int(r[1])
		r_key=KV_EXISTS_GROUPID % gid
		r_redis.set(r_key,"")
		
		r_key=KV_NOSPEAKING_GROUPID % gid
		r_redis.set(r_key,is_group_forbidden_speaking)
		
		sql='select userid from group_users where groupid=%s' % gid
		res2=sql_query(sql)
		for r2 in res2:
			uid=r2[0]
			r_key=KV_USERID_IN_GROUPID % (gid,uid)
			r_redis.set(r_key,"")
			
	#查询所有用户名的到userid和userid到用户名的映射关系
	sql='select id,username from user'
	res=sql_query(sql)
	for r in res:
		uid=r[0]
		name=r[1]
		r_key=KV_USERNAME_GET_USERID % name	
		r_redis.set(r_key,uid)	

		r_key=KV_USERID_GET_USERNAME % uid	
		r_redis.set(r_key,name)	

	#查询所有群名的到groupid、groupid到群名、群主userid到群groupid、群groupid到群主userid的映射关系
	sql='select id,name,own_userid from `group`'
	res=sql_query(sql)
	for r in res:
		gid=r[0]
		gname=r[1]
		own_uid=r[2]
		r_key=KV_GROUPID_GET_GROUPNAME % gid
		r_redis.set(r_key,gname)	
		r_key=KV_GROUPID_GET_OWN_USERID % gid 
		r_redis.set(r_key,own_uid)	
		r_key=KV_OWNUSERID_OWN_GROUPID % (own_uid,gid)
		r_redis.set(r_key,"")	


	#查询用户包含的群ID列表
	sql='select id from user'
	res=sql_query(sql)
	for r in res:
		uid=r[0]
		sql='select groupid from group_users where userid=%s' % uid
		res2=sql_query(sql)
		for r2 in res:
			gid=r2[0]
			r_key=KV_USERID_IN_GROUPID % (uid,gid)
			r_redis.set(r_key,"")

#启动服务
def start_chat_server():
	#生成SSL上下文
	context=ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)	
	context.load_cert_chain(certfile=CERTFILE,keyfile=KEYFILE)
	#创建socket
	with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as s: 
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		#绑定
		s.bind((host,port))
		#监听
		s.listen(10000)
		#设置非阻塞
		s.setblocking(False)
		#将socket包装成SSL
		with context.wrap_socket(s,server_side=True) as ssl_s:
			#注册socket fileno到epoll上面
			epoll.register(ssl_s.fileno(),select.EPOLLIN)
			#文件描述符到socket字典
			fd_to_socket[ssl_s.fileno()]=ssl_s
			print("启动聊天服务器%s:%s！！！" %(host,port))
			print("等待客户端的连接...")
			#加载基础信息数据到redis
			print("加载基础信息数据到redis...")
			load_data2redis()
			while True:
				#轮询注册的事件集合，返回文件句柄，对应的事件
				events=epoll.poll(10)
				if not events:
					continue
				#print("有"+str(len(events))+"个新事件，开始处理......")
				#如果有事件发生,迭代读取事件,并且处理事件
				for fd,event in events: 
					c_sock=fd_to_socket[fd]
					#判断是否为服务器监听的socket
					if fd == ssl_s.fileno():
						#处理新连接
						try:
							new_conn,addr=ssl_s.accept()
						except ssl.SSLError as e:
							print("套接字异常错误:",e)
							continue
						new_conn.setblocking(False)
						c_fd=new_conn.fileno()
						#注册到epoll中
						try:
							epoll.register(c_fd,select.EPOLLOUT)
						except:
							epoll.unregister(c_fd)
							new_conn.close()
							continue
						#添加到fd_to_socket字典表中
						fd_to_socket[c_fd]=new_conn
						message_queue[c_fd]=queue.Queue()
					#读事件
					elif event & select.EPOLLIN:
						#条用epollin处理函数
						try:
							handle_epollin(fd,c_sock)
						except ssl.SSLError as e:
							print("套接字异常错误:",e)
							continue
					#写事件
					elif event & select.EPOLLOUT:	
						#条用epollout处理函数
						handle_epollout(fd,c_sock)
					#关闭事件
					elif event & select.EPOLLHUP:
						close_clear_all(c_sock,fd)

if __name__=='__main__':
	start_chat_server()
