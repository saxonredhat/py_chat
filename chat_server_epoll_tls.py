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
KV_EXISTS_USERID=u'kv_exists_userid:userid:%s'
KV_GROUPID_GET_GROUPNAME=u'kv_groupid_get_groupname:groupid:%s'
KV_GROUPID_GET_OWN_USERID=u'kv_groupid_get_own_userid:groupid:%s'
KV_USERID_IN_GROUPID=u'kv_userid_in_groupid:userid:%s:groupid:%s'
KV_OWNUSERID_OWN_GROUPID=u'kv_ownuserid_own_groupid:own_userid:%s:groupid:%s'
KV_USERID_GET_USERNAME=u'kv_userid_get_username:userid:%s'
KV_USERID_ISFRIEND_USERID=u'kv_userid_isfriend_userid:userid:%s:userid:%s'
KV_IS_FORBIDDEN_SPEAKING_GROUPID=u'kv_is_forbidden_speaking_groupid:groupid:%s'
KV_IS_FORBIDDEN_SPEAKING_USERID_IN_GROUPID=u'kv_is_forbidden_speaking_userid_in_groupid:groupid:%s:userid:%s'
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
	port=8888

#无参数命令
noargs_cmd=['listfriends','lf','listgroups','lg','listallgroup','lag','close','logout']

#带参数命令
args_cmd=['msg','m','gmsg','gm','addfriend','af','deluser','du','entergroup','eng','exitgroup','exg','kickout','ko','reject','rj','accept','ac','creategroup','cg','delgroup','dg','listgroupusers','lgu','auth','echo','nospkuser','nsu','spkuser','su','nospkgroup','nsg','spkgroup','sg']

#命令帮助提示
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
	|gmsg/gm       GID    MESSAGE  (给群发送消息)   |
	|kickout/ko    GID    UID      (移出群用户)     |
	|nospkuser/nsu GID    UID      (禁言群用户)     |
	|spkuser/su    GID    UID      (取消禁言群用户) |
	 ===============================================
	|命令                参数    (说明)             |
	 ===============================================
	|echo                MESSAGE (回显消息)         |
	|infome/im           MESSAGE (回显消息)         |
	|adduser/au          UID     (添加好友)         |
	|deluser/du          UID     (删除好友)         |
	|creategroup/cg      GNAME   (建群)             |
	|delgroup/dg         GID     (删群)             |
	|entergroup/eng      GID     (加群)             |
	|exitgroup/exg       GID     (退群)             |
	|nospkgroup/nsg      GID     (禁言群)           |
	|spkgroup/sg         GID     (取消禁言群)       |
	|reject/rj           RID     (同意加好友|进群)  |
	|accept/ac           RID     (拒绝加好友|进群)  |
	|listgroupusers/lgu  GID     (列出群成员信息)   |
	|infouser/iu         UID     (显示用户信息)     |
	 ===============================================
	|命令                (说明)                     |
	 ===============================================
	|infome/im           (显示当前用户信息)         |
	|listfriends/lf      (列出好友)                 |
	|listusers/lu        (列出系统所有用户)         |
	|listgroups/lg       (列出创建的群|列出加入的群)|
	|listallgroup/lag    (列出系统所有的群)         |
	|logout              (退出)                     |
	|close               (关闭对话)                 |
	 ===============================================
"""

#userid对应客户端socket字典
userid_to_socket={}

#认证成功文件描述符对应socket字典
auth_fd_to_socket={}

#文件描述符对应socket的字典
fd_to_socket={}

#文件描述符对应命令统计次数的字典
fd_to_cmd_count={}

#文件描述对应消息队列的字典
fd_to_message_queue={}

#创建epoll对象
epoll=select.epoll()

#获取自定义的时间string
def get_custom_time_string(t_format="%Y-%m-%d %X"):
	return time.strftime(t_format)

#获取socket对应的userid
def get_userid_of_socket(sock):
	for userid,usock in userid_to_socket.items():
		if sock == usock:
			return userid

#获取userid对应的socket
def get_socket_of_userid(userid):
	if userid in userid_to_socket.keys():
		return userid_to_socket[userid]

#判断
def user_is_online(userid):
	for userid in userid_to_socket.items():
		return True
	return False

#删除字典userid_to_socket中对应键值
def del_userid_to_socket(sock):
	if get_userid_of_socket(sock):
		del userid_to_socket[userid]

#转换发送的数据成二进制数据并发送
def send_data(sock,data):
	b_data=bytes(data,encoding='utf-8')
	b_datasize=struct.pack('H',len(b_data))
	try:
		sock.sendall(b_datasize+b_data)
		return True
	except:
		sock.close()
		return False

def sql_query(sql):
	db=pymysql.connect(**db_config)
	cursor=db.cursor()
	results=[]
	try:
		print(u'[ %s ] 执行查询操作,SQL->[ %s ]' % (get_custom_time_string(),sql))	
		cursor.execute(sql)
		results=cursor.fetchall()
	except Exception as e:
		print(u'[ %s ] 执行查询发生异常->[ %s ],SQL->[ %s ]' % (get_custom_time_string(),e,sql))	
	finally:
		cursor.close()
		db.close()
	return results

def sql_dml(sql):
	db=pymysql.connect(**db_config)
	cursor=db.cursor()
	try:
		print(u'[ %s ] 执行DML操作,SQL->[ %s ]' % (get_custom_time_string(),sql))	
		cursor.execute(sql)	
		db.commit()
		return True
	except Exception as e:
		print(u'[ %s ] 执行DML操作,发生异常->[ %s ],进行回滚,SQL->[ %s ]' % (get_custom_time_string(),e,sql))
		db.rollback()
	finally:
		cursor.close()
		db.close()
	return False

def userid_is_exists(userid):
	r_key=KV_EXISTS_USERID % userid 
	if not r_redis.exists(r_key):
		sql='select id from user where id=%s' % userid 
		if not sql_query(sql):
			r_redis.set(r_key,"")
			return False
	return True

def groupid_is_exists(groupid):
	#读取redis
	r_key=KV_EXISTS_GROUPID % groupid
	if not r_redis.exists(r_key):
		sql='select id from `group` where id=%s' % groupid
		if not sql_query(sql):
			return False
	return True

def get_username_of_userid(userid):
	#读取redis
	r_key=KV_USERID_GET_USERNAME % userid	
	if not r_redis.exists(r_key):
		sql='select username from user where id=%s' % userid
		res=sql_query(sql)
		if res:
			q_username=res[0][0]
			r_redis.set(r_key,q_username)
			return q_username 
		return ''
	else:
		return r_redis.get(r_key).decode('utf-8')

def get_groupname_of_groupid(groupid):
	r_key=KV_GROUPID_GET_GROUPNAME % groupid
	if r_redis.exists(r_key) and r_redis.get(r_key):
		groupname=r_redis.get(r_key).decode('utf-8')
	else:
		sql='select name from `group` where id=%s' % groupid
		res=sql_query(sql)
		if res:
			groupname=res[0][0]
			r_redis.set(r_key,groupname)	
		else:
			groupname=''
	return groupname

def user_is_friend_of_user(userid,userid2):
	r_key=KV_USERID_ISFRIEND_USERID % (userid,userid2)
	if not r_redis.exists(r_key):
		sql='select userid from user_users where userid=%s and friend_userid=%s' % (userid,userid2)
		res=sql_query(sql)
		if not res:
			return False 
		r_redis.set(r_key,"")
	return True

def user_is_exists_add_user_request(userid,userid2):
	sql='select id from user_req where userid=%s and to_userid=%s and type=1 and status=0' % (userid,userid2)
	if sql_query(sql):
		return True
	return False

def user_is_exists_add_group_request(userid,groupid):
	sql='select add_groupid from user_req where userid=%s and add_groupid=%s and status=0' % (userid,groupid)
	res=sql_query(sql)
	if res:
		return True
	return False
	
def get_own_userid_of_groupid(groupid):
	r_key=KV_GROUPID_GET_OWN_USERID % groupid
	if r_redis.exists(r_key) and r_redis.get(r_key):
		own_userid=int(r_redis.get(r_key).decode('utf-8'))
	else:
		sql='select own_userid from `group` where id=%s' % groupid
		res=sql_query(sql)
		if res:
			own_userid=int(res[0][0])
			r_redis.set(r_key,own_userid)
		else:
			own_userid=-1
	return own_userid

def userid_is_exists_in_group(userid,groupid):
	r_key=KV_USERID_IN_GROUPID % (userid,groupid)
	if not r_redis.exists(r_key):
		sql='select id from group_users where groupid=%s and userid=%s' % (groupid,userid)
		res=sql_query(sql)
		if not res:
			r_redis.set(r_key,"")
			return False 
	return True 

def get_counts_of_group(groupid):
	sql='select count(1) from group_users where groupid=%s' % groupid
	return sql_query(sql)[0][0]

def get_create_group_limits_of_userid(userid):
	sql='select group_limits from user where id=%s' % userid 
	return sql_query(sql)[0][0]

def get_has_group_counts_of_userid(userid):
	sql='select count(1) from `group` where own_userid=%s' % userid 
	return sql_query(sql)[0][0]

def group_is_forbidden_speaking(groupid):
	r_key=KV_IS_FORBIDDEN_SPEAKING_GROUPID % groupid
	if not r_redis.exists(r_key):
		sql='select is_group_forbidden_speaking from `group` where id=%s' % groupid
		res=sql_query(sql)
		is_group_forbidden_speaking=int(res[0][0])
		r_redis.set(r_key,is_group_forbidden_speaking)
	else:
		is_group_forbidden_speaking=int(r_redis.get(r_key).decode('utf-8'))

	if is_group_forbidden_speaking==1:
		return True 
	return False

def user_is_forbidden_speaking_in_group(userid,groupid):
	r_key=KV_IS_FORBIDDEN_SPEAKING_USERID_IN_GROUPID % (groupid,userid)
	if not r_redis.exists(r_key):
		sql='select is_forbidden_speaking from group_users where groupid=%s and userid=%s' % (groupid,userid)
		res=sql_query(sql)
		is_forbidden_speaking=int(res[0][0])
		r_redis.set(r_key,is_forbidden_speaking)
	else:
		is_forbidden_speaking=int(r_redis.get(r_key).decode('utf-8'))

	if is_forbidden_speaking == 1:
		return True
	return False

def get_userids_of_group(groupid):
	userids_list=[]
	r_key=LIST_USERIDS_OF_GROUPID % groupid
	if r_redis.llen(r_key):
		for uid in r_redis.lrange(r_key,0,-1):
			userids_list.append(int(uid.decode('utf-8')))
	else:
		sql='select userid from group_users where groupid=%s' % groupid
		res=sql_query(sql)
		for r in res:
			to_userid=r[0]
			userid_list.append(to_userid)
			r_redis.rpush(r_key,to_userid)
	return userids_list

def user_auth(userid,password):
	sql="select username,password from user where id=%s" % userid
	res=sql_query(sql)
	if res:
		q_username=res[0][0]
		q_password=res[0][1]
		if q_password == password:
			return True
		else:
			return False
	return False 

def push_messages(sock,userid):
	#获取用户拥有的群的id
	groupids_list=[]
	sql='select groupid from group_users where userid=%s' % userid
	res=sql_query(sql)
	for r in res:
		groupids_list.append(r[0])

	messages_list=[]
	#是否收到群消息
	for groupid in groupids_list:
		#判断是否需要推送给当前用户
		r_key=LIST_GROUPMESSAGES_OF_USERID_IN_GROUPID % (groupid,userid)
		for seq in range(0,r_redis.llen(r_key)):
			group_message=r_redis.lpop(r_key).decode('utf-8')
			messages_list.append(group_message)
				
	#是否收到用户消息
	#查询redis数据库是否有该用户的数据
	r_key=LIST_USERMESSAGES_OF_USERID % userid
	for seq in range(0,r_redis.llen(r_key)):	
		user_message=r_redis.lpop(r_key).decode('utf-8')
		messages_list.append(msg)

	#是否收到提醒消息
	sql='select id,content from user_notice where status=0 and userid=%s' % userid  
	res=sql_query(sql)
	rowids_list=[]
	for r in res:
		rowid=r[0]
		message_content=r[1]
		messages_list.append(message_content)	
		rowids_list.append(rowid)
	if rowids_list:	
		rowids=','.join(rowids_list)
		sql='update user_notice set status=1 where id in (%s)' % rowids
		sql_dml(sql)
	
	for message in sorted(messages_list):
		send_data(sock,message)
	
	#收到加好友消息
	sql='select id,userid from user_req where add_userid=%s and status=0' % userid 
	res=sql_query(sql)
	for r in res:
		req_id=r[0]
		req_userid=r[1]
		req_username=get_username_of_userid(req_userid)
		send_data(sock,u"[ %s ]\n【系统消息】用户[ %s|ID:%s ]向您申请添加好友!\n同意：accept %s \n拒绝：reject %s" % (get_custom_time_string(),req_username,req_userid,req_id,req_id))

	#收到加群消息
	sql='select ur.id,ur.userid,g.id,g.name from user_req ur join `group` g on g.id=ur.add_groupid where g.own_userid=%s and ur.status=0' % userid
	res=sql_query(sql)
	for r in res:
		req_id=r[0]
		req_userid=r[1]
		req_username=get_username_of_userid(req_userid)
		groupid=r[2]
		groupname=r[3]
		send_data(sock,u'[ %s ]\n【系统消息】用户[ %s|ID:%s ]向你申请加入群[ %s|ID:%s ]\n同意：accept %s \n拒绝：reject %s' % (get_custom_time_string(),req_username,req_userid,groupname,groupid,req_id,req_id))


#用户给好友发送信息
def send_user_message(sock,args_list):
	if len(args_list)<2:
		send_data(sock,u'[ %s ]【系统提示】发送消息的操作参数错误' % get_custom_time_string())
		return	
	send_userid=get_userid_of_socket(sock)
	message_content=' '.join(args_list[1:])
	to_userid=args_list[0]
	to_username=get_username_of_userid(to_userid)

	if not userid_is_exists(to_userid):
		send_data(sock,u'[ %s ]【系统消息】 用户UID[ %s ]在系统中不存在' % (get_custom_time_string(),to_userid))
		return

	send_userid=get_userid_of_socket(sock)
	send_username=get_username_of_userid(send_userid)
	user_message=u'[ %s ]\n【用户消息】[ %s|UID:%s ]: %s' % (get_custom_time_string(),send_username,send_userid,message_content)
	#判断是否为自己发送给自己
	if send_userid == int(to_userid):
		send_data(sock,user_message)
		return

	#判断当前发送消息的接收方是否为好友，如果不是拒绝发送消息
	if user_is_friend_of_user(send_userid,to_userid):
		send_data(sock,u'[ %s ]【系统消息】 用户[ %s|UID:%s ]不是您的好友,不能发送消息!' % (get_custom_time_string(),to_username,to_userid))
		return

	to_sock=get_socket_of_userid(to_userid)
	if to_sock:
		send_data(to_sock,send_user_message)
	else:
		r_key=LIST_USERMESSAGES_OF_USERID % to_userid
		r_redis.rpush(r_key,user_message) 
	send_data(sock,user_message)


def send_group_message(sock,args_list):
	if len(args_list)<2:
		send_data(sock,u'【系统提示】发送群消息的操作参数错误!')
		return	
	groupid=args_list[0]
	message_content=' '.join(args_list[1:])

	#判断群是否存在
	if not groupid_is_exists(groupid):
		send_data(sock,"【系统提示】群GID[ %s ]不存在!" % groupid)
		return

	#获取群主ID
	own_userid=get_own_userid_of_groupid(groupid)
	#获取群名
	groupname=get_own_userid_of_groupid(groupid)
	#获取发送者的userid
	send_userid=get_userid_of_socket(sock)
	#判断是否为群成员，非成员不能发送消息
	if not userid_is_exists_in_group(send_userid,groupid):
		send_data(sock,"【系统提示】您非群[ %s|ID:%s ]成员，不能发送群信息!" % (groupname,groupid))
		return

	#判断是否是群主，非群主判断是否群禁言,判断是否用户被禁言
	if send_userid != own_userid:
		#是否当前群禁言
		if group_is_forbidden_speaking(groupid):
			send_data(sock,u'【系统消息】 群[ %s|GID:%s ]已被禁言!' % (groupname,groupid))
			return 

		#是否当前用户被禁言
		if user_is_forbidden_speaking_in_group(groupid,send_userid):
			send_data(sock,u'【系统消息】 您已被群[ %s|GID:%s ]的群主禁言!' % (groupname,groupid))
			return 

	#获取群用户，发送群消息
	send_username=get_username_of_userid(send_userid)
	group_message='[ %s ]\n【群消息】[ %s|GID:%s ][ %s|UID:%s ]:%s' % (get_custom_time_string(),groupname,groupid,send_username,send_userid,message_content)

	#获取群用户，发送群消息
	userids_list=get_userids_of_group(groupid)
	for to_userid in userids_list:
		#判断用户是否在线
		if user_is_online(to_userid):
			to_sock=get_socket_of_userid(to_userid)
			send_data(to_sock,group_message)
		else:
			r_key=LIST_GROUPMESSAGES_OF_USERID_IN_GROUPID % (groupid,to_userid)
			r_redis.rpush(r_key,group_message)

def add_friend(sock,args_list):
	if len(args_list)!=1:
		send_data(sock,u'【系统提示】添加好友的命令错误!')
		return
	req_userid=get_userid_of_socket(sock)
	req_username=get_username_of_userid(req_userid)
	to_userid=args_list[0]
	to_username=get_username_of_userid(to_userid)

	#判断userid是否存在
	if not userid_is_exists(to_userid):
		send_data(sock,u"【系统提示】系统UID[ %s ]不存在!" % to_userid)
		return

	#判断是否添加的是自己
	if req_userid == to_userid:
		send_data(sock,u"【系统提示】无需添加自己为好友!")
		return 

	#判断是否已经添加为好友,或者是否已经存在申请请求
	if user_is_friend_of_user(req_userid,to_userid):
		send_data(sock,u"【系统提示】您和用户[ %s|UID:%s ]已经是好友关系，不需要再次添加" % (to_username,to_userid))
		return


	if user_is_exists_add_user_request(userid,to_userid):
		send_data(sock,u"【系统提示】您已经向[ %s|UID:%s ]发送过好友申请，无需再次发送" % (to_username,to_userid)) 
		return
	
	to_sock=get_socket_of_userid(to_userid)
	sql='insert into user_req(userid,type,to_userid,status,created_at) values(%s,1,%s,0,now())' % (userid,to_userid)
	sql_dml(sql)
	if to_sock:
		sql='select id from user_req where userid=%s and to_userid=%s and status=0 LIMIT 1' %  (userid,to_userid)
		res=sql_query(sql)
		if not res:
			return
		req_id=res[0][0]
		send_data(to_sock,u"[ %s ]【系统消息】用户%s向您申请添加好友请求!\n同意：\naccept %s \n拒绝：\nreject %s" % (get_custom_time_string(),req_username,req_id,req_id))
	send_data(sock,u'[ %s ]【系统消息】已向该用户%s发送添加好友申请' % (get_custom_time_string(),to_username))

def delete_friend(sock,args_list):
	if len(args_list)!=1:
		send_data(sock,u'【系统提示】删除好友的命令错误!')
		return
	req_userid=get_userid_of_socket(sock)	
	req_username=get_username_of_userid(req_userid)
	to_userid=args_list[0]
	to_username=get_username_of_userid(to_userid)
	if not userid_is_exists(to_userid):
		send_data(sock,u"【系统提示】系统UID不存在!" % to_userid)
		return

	#判断是否已经是好友
	if not user_is_friend_of_user(userid,to_userid):
		send_data(sock,u"【系统提示】您和用户[ %s|UID:%s ]不是好友关系!" % (to_username,to_userid))
		return

	###开始清除redis###
	r_key=KV_USERID_ISFRIEND_USERID % (userid,to_userid)
	r_redis.delete(r_key)
	r_key=KV_USERID_ISFRIEND_USERID % (to_userid,userid)
	r_redis.delete(r_key)
	###结束清除redis###

	#双向解除好友关系
	sql='delete from user_users where userid=%s and friend_userid=%s' % (userid,to_userid) 
	sql_dml(sql)
	sql='delete from user_users where userid=%s and friend_userid=%s' % (to_userid,userid) 
	sql_dml(sql)

	send_data(sock,u"【系统消息】您和用户[ %s ]已经解除好友关系!" % (to_username))
	
	
	#通知对方
	to_sock=get_socket_of_userid(to_userid)
	notice_content='[ %s ]【系统消息】用户[ %s ]已和你您解除好友关系!' % (get_custom_time_string(),req_username)
	if to_sock:
		send_data(to_sock,notice_content)
	else:
		notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (to_userid,notice_content)
		sql_dml(notice_sql)
	

def enter_group(sock,args_list):
	if len(args_list)!=1:
		send_data(sock,u'【系统提示】加群的操作参数错误')
		return

	req_userid=get_userid_of_socket(sock)
	groupid=args_list[0]
	#判断群是否存在
	if not groupid_is_exists(groupid):
		send_data(sock,"【系统提示】该群ID号[ %s ]不存在!" % groupid)
		return 

	#获取群主ID
	own_userid=get_own_userid_of_groupid(groupid)
	#获取群名
	groupname=get_groupname_of_groupid(groupid)
	
	#判断用户是否已经加入
	if userid_is_exists_in_group(req_userid,groupid):
		send_data(sock,u'【系统提示】您已经在群[ %s|GID:%s ]中,无需加入!'% (groupname,groupid))
		return

	#判断是否已经存在加入该群的申请请求
	if user_is_exists_add_group_request(req_userid,groupid):	
		send_data(sock,u'【系统提示】您已经申请过加入群[ %s|GID:%s ]，请等待回复!'% (groupname,groupid))
		return
	
	#添加加群请求
	sql='insert into user_req(userid,type,add_groupid,status,created_at) values(%s,2,%s,0,now())' % (req_userid,groupid)
	sql_dml(sql)
	sql='select id from user_req where userid=%s and add_groupid=%s and status=0 LIMIT 1' % (req_userid,groupid)
	res=sql_query(sql)
	if not res:
		return
	req_id=res[0][0]

	#判断群主是否在在线，在线直接发送加群提醒，如果不在线，群主通过user_req可以看到加群消息	
	req_username=get_username_of_userid(req_userid)
	if user_is_online(own_userid):
		own_sock=get_socket_of_userid(own_userid)
		send_data(own_sock,u'[ %s ]【系统消息】用户[ %s ]申请加入群[ %s|GID:%s ]\n同意：\naccept %s \n拒绝：\nreject %s' % (get_custom_time_string(),req_username,groupname,groupid,req_id,req_id))
	send_data(sock,u'【系统消息】您刚刚申请了加入群[ %s|GID:%s ]，请等待回复!'% (groupname,groupid))

def exit_group(sock,args_list):
	if len(args_list)!=1:
		send_data(sock,u'【系统提示】退群的操作参数错误')
		return

	req_userid=get_userid_of_socket(sock)
	req_username=get_username_of_userid(req_userid)
	groupid=args_list[0]
	#判断群是否存在
	if not groupid_is_exists(groupid):
		send_data(sock,"【系统提示】该群ID号[ %s ]不存在!" % groupid)
		return 

	#获取群主ID
	own_userid=get_own_userid_of_groupid(groupid)
	#获取群名
	groupname=get_groupname_of_groupid(groupid)

	#判断用户是否已经加入
	if userid_is_exists_in_group(req_userid,groupid):
		send_data(sock,u'【系统提示】您不在群[ %s|ID:%s ]中,无需退出!'% (groupname,groupid))
		return

	#判断是否为群主,群主无法退群
	if req_userid == own_userid:
		send_data(sock,u'【系统提示】您是群[ %s|ID:%s ]的群主，无法退群!' % (groupname,groupid))
		return

	###开始清除redis###
	r_key=KV_USERID_IN_GROUPID % (req_userid,groupid)
	r_redis.delete(r_key)
	r_key=LIST_USERIDS_OF_GROUPID % groupid
	r_redis.delete(r_key)
	LIST_GROUPMESSAGES_OF_USERID_IN_GROUPID % (groupid,req_userid)
	r_redis.delete(r_key)
	###结束清除redis###

	#退群操作
	sql='delete from group_users where userid=%s and groupid=%s' % (req_userid,groupid)
	sql_dml(sql)

	send_data(sock,u'【系统消息】您已退出群[ %s|GID:%s ]!' % (groupname,groupid))

	#判断群主是否在在线，在线直接发送退群提醒，如果不在线，把退群消息加入到user_notice	
	notice_content=u'[ %s ]\n【系统消息】用户[ %s|UID:%s ]退出群[ %s |ID号:%s] ' % (get_custom_time_string(),req_userid,req_username,groupname,groupid)
	if user_is_online(own_userid):
		own_sock=get_socket_of_userid(own_userid)
		send_data(own_sock,notice_content)
	else:
		notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (own_userid,notice_content)
		sql_dml(notice_sql)

def accept_request(sock,args_list):
	if len(args_list)!=1:
		send_data(sock,u'【系统提示】同意请求的操作参数错误')
		return	
	accept_userid=get_userid_of_socket(sock)
	req_id=args_list[0]
	sql='select id,type,userid,add_userid,add_groupid from user_req where id=%s and status=0' % req_id
	res=sql_query(sql)
	if not res:
		send_data(sock,u'【系统提示】请求的RID号[ %s ]不存在' % req_id)
		return
	req_type=res[0][1]
	req_userid=res[0][2]
	add_userid=res[0][3]
	add_groupid=res[0][4]

	#判断是加群还是添加好友 
	#加好友
	if req_type == 1 and add_userid:
		if accept_userid != add_userid:
			send_data(sock,u'【系统提示】您无权执行该操作!')
			return 
		add_username=get_username_of_userid(add_userid)
			
		#互加好友操作
		sql='insert into user_users(userid,friend_userid,created_at) values(%s,%s,now())' % (req_userid,add_userid)
		sql_dml(sql)
		sql='insert into user_users(userid,friend_userid,created_at) values(%s,%s,now())' % (add_userid,req_userid)
		sql_dml(sql)

		send_data(sock,u'[ %s ]\n【系统消息】您已同意添加用户[ %s|UID:%s ]为好友' % (get_custom_time_string(),add_username,add_userid))
		notice_content=u'[ %s ]\n【系统消息】用户[ %s|UID:%s ]已经同意添加您为好友!' % (get_custom_time_string(),add_username,add_userid)

	#加群
	if req_type == 2 and add_groupid:
		if accept_userid != own_userid:
			send_data(sock,u'【系统提示】您非该群的群主，无权执行该操作!')
			return 

		req_username=get_username_of_userid(req_userid)
		own_userid=get_own_userid_of_groupid(add_groupid)
		own_username=get_username_of_userid(group_own_userid)
		groupname=get_groupname_of_groupid(add_groupid)
		
		#把用户加入群
		sql='insert into group_users(userid,groupid,created_at) values(%s,%s,now())' % (req_userid,add_groupid)
		sql_dml(sql)

		send_data(sock,u'[ %s ]\n【系统消息】您已同意用户[ %s|UID:%s ]加入群[ %s|GID:%s ]!' %(get_custom_time_string(),req_username,req_userid,groupname,groupid) )
		notice_content=u"[ %s ]\n【系统消息】群主[ %s|UID:%s ]已经同意你加入群[ %s|GID:%s ]!" % (get_custom_time_string(),own_username,own_userid,groupname,groupid)

	#判断该请求的用户是否在线，如果在线直接提醒用户，不在线插入提醒信息到数据库
	if user_is_online(req_userid):
		req_sock=get_socket_of_userid(req_userid)
		send_data(req_sock,notice_content)
	else:
		notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (req_userid,notice_content)
		sql_dml(notice_sql)

	#更新请求状态
	sql='update user_req set status=1 where id=%s' % req_id
	sql_dml(sql)

def reject_requset(sock,args_list):
	if len(args_list)!=1:
		send_data(sock,u'【系统提示】拒绝请求的操作参数错误')
		return	
	reject_userid=get_userid_of_socket(sock)
	req_id=args_list[0]
	sql='select id,type,userid,add_userid,add_groupid from user_req where id=%s and status=0' % req_id
	res=sql_query(sql)
	if not res:
		send_data(sock,u'【系统提示】请求的RID号[ %s ]不存在' % req_id)
		return
	req_type=[0][0]
	req_userid=res[0][1]
	add_userid=res[0][2]
	add_groupid=res[0][3]

	#如果是添加好友 
	if req_type == 1 and add_userid:
		if reject_userid != add_userid:
			send_data(sock,u'【系统提示】您无权执行该操作!')
			return 

		add_username=get_username_of_userid(add_userid)
		send_data(sock,u'[ %s ]\n【系统消息】您已拒绝用户[ %s|UID:%s ]加好友请求！' % (get_custom_time_string(),add_username,add_userid))
		notice_content=u'[ %s ]\n【系统消息】用户[ %s|UID:%s ]已经拒绝您的好友申请!' % (get_custom_time_string(),add_username,add_userid)

	#如果是添加群
	if req_type == 1 and add_groupid:
		if reject_userid != own_userid:
			send_data(sock,u'【系统提示】您非该群的群主，无权执行该操作!')
			return 

		req_username=get_username_of_userid(req_userid)
		own_userid=get_own_userid_of_groupid(add_groupid)
		own_username=get_username_of_userid(group_own_userid)
		groupname=get_groupname_of_groupid(add_groupid)

		send_data(sock,u'[ %s ]\n【系统消息】您已拒绝用户[ %s|UID:%s ]申请加群[ %s|GID:%s ]的请求!' %(get_custom_time_string(),req_username,req_userid,groupname,groupid))
		notice_content=u"[ %s ]\n【系统消息】群主[ %s|UID:%s ]已经同意你加入群[ %s|GID:%s ]!" % (get_custom_time_string(),own_username,own_userid,groupname,groupid)

	#判断用户是否在线，如果在线直接提醒用户，不在线插入提醒信息到数据库
	if user_is_online(req_userid):
		req_sock=get_socket_of_userid(req_userid)
		send_data(req_sock,notice_content)
	else:
		notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (req_userid,notice_content)
		sql_dml(notice_sql)

	#更新请求状态
	sql='update user_req set status=1 where id=%s' % req_id
	sql_dml(sql)
		

def list_friends(sock,args_list):
	userid=get_userid_of_socket(sock)
	friends=[]	
	sql='select u.id,u.username from user_users uu join user u on uu.friend_userid=u.id  where uu.userid=%s' % userid
	res=sql_query(sql)
	for r in res:
		friend_userid=r[0]
		friend_username=r[1]
		friend_info='- [ %s|UID:%s ]' % (friend_username,friend_userid)
		if user_is_online(friend_userid):
			friend_info+='(在线)'
		else:
			friend_info+='(离线)'
		friends.append(friend_info)

	if friends:
		firends_info_msg=u'当前好友列表:\n%s' % '\n'.join(friends)
	else:
		 firends_info_msg=u'当前没有添加好友!'
	send_data(sock,firends_info_msg)

def list_groups(sock,args_list):
	userid=get_userid_of_socket(sock)
	add_groups=[]
	has_groups=[]
	#查看当前用户加入的群
	sql=u'select gg.id,gg.name,gg.own_userid,count(1) from group_users ggu join `group` gg on ggu.groupid=gg.id where gg.id in (select g.id from group_users gu join `group` g on gu.groupid=g.id and gu.userid=%s) group by gg.id' % userid
	res=sql_query(sql)
	for r in res:
		groupid=r[0]
		groupname=r[1]	
		own_userid=r[2]
		group_people_counts=r[3]
		group_info='- [ %s|GID:%s ](%s)' % (groupname,groupid,group_people_counts) 
		if userid==own_userid:
			has_groups.append(group_info)
		else:
			add_groups.append(group_info)

	if has_groups:
		has_groups_info='\n'.join(has_groups)
	else:
		has_groups_info='-'

	if add_groups:
		add_groups_info='\n'.join(add_groups)
	else:
		add_groups_info='-'
		
	send_data(sock,"您拥有的群:\n%s\n您加入的群:\n%s\n" % ('\n'.join(has_groups),'\n'.join(add_groups)))

def list_all_groups(sock,args_list):
	userid=get_userid_of_socket(sock)
	#查看当前用户拥有的群
	groups=[]
	sql='select id,name from `group`' 
	res=sql_query(sql)
	for r in res:
		groupid=r[0]
		groupname=r[1]
		groupcounts=get_counts_of_group(groupid)
		own_userid=get_own_userid_of_groupid(groupid)
		if userid==own_userid:
			groupinfo=u'- [ %s ]\t%s\t%s\t您是群主' % (groupname,groupid,groupcounts)
		else:
			if userid_is_exists_in_group(userid,groupid):
				groupinfo=u'- [ %s ]\t%s\t%s\t已加入' % (groupname,groupid,groupcounts)
			else:
				groupinfo=u'- [ %s ]\t%s\t%s\t未加入' % (groupname,groupid,groupcounts)
		groups.append(groupinfo)

	send_data(sock,"当前所有的群:\n  群名\tGID\t人数\t是否加入\n%s" % '\n'.join(groups))

def list_group_users(sock,args_list):
	if len(args_list)!=1:
		send_data(sock,u'【系统提示】列出群成员的操作参数错误!')
		return	
	groupid=args_list[0]

	#判断群是否存在
	if not groupid_is_exists(groupid):
		send_data(sock,"【系统提醒】该群GID[ %s ]不存在!" % groupid)
		return 

	req_userid=get_userid_of_socket(sock)
	own_userid=get_own_userid_of_groupid(groupid)
	groupname=get_groupname_of_groupid(groupid)
	groupcounts=get_counts_of_group(groupid)

	#判断是否为群成员，非成员不能查看群信息
	if not userid_is_exists_in_group(req_userid,groupid):
		send_data(sock,"【系统提示】您非群[ %s|GID:%s ]成员，不能查看群成员信息!" % (groupname,groupid))
		return
	

	#群信息	
	groupinfo=u'群:[ %s|GID:%s ]（人数:%s)' % (groupname,groupid,groupcounts)
	#查看群成员信息
	userids_list=get_userids_of_group(groupid)
	userinfos_list=[]
	for userid in userids_list:
		username=get_username_of_userid(userid)
		if userid == own_userid:
			userinfo="- [ %s|UID:%s ](群主)" % (username,userid)
		else:
			userinfo="- [ %s|UID:%s ](成员)" % (username,userid)
		if user_is_online(userid):
			userinfo+='(在线)'
		else:
			userinfo+='(离线)'
		userinfos_list.append(userinfo)
	send_data(sock,'%s\n%s' %(groupinfo,'\n'.join(userinfos_list)))

def create_group(sock,args_list):
	if len(args_list)!=1:
		send_data(sock,u'【系统提示】创建群的操作参数错误')
		return	
	create_userid=get_userid_of_socket(sock)
	groupname=args_list[0]

	#判断该用户的群是否已经达到上限	
	has_group_counts=get_has_group_counts_of_userid(userid)
	create_group_limits=get_create_group_limits_of_userid(userid)
	if has_group_counts >= create_group_limits:
		send_data(sock,u'【系统提示】您创建的群数量已达上限数为%s,当前拥有群数为%s' % (create_group_limits,has_group_counts))
		return

	#建群
	sql='insert into `group`(name,create_userid,is_empty,created_at) values("%s",%s,0,now())' % (groupname,create_userid)
	create_group_result=sql_dml(sql)

	#获取刚创立的群id
	sql='select id from `group` where create_userid=%s and is_empty=0' % create_userid;
	groupid=res[0][0]

	#把当前用户加到群内
	sql='insert into `group_users`(userid,groupid,created_at) values(%s,%s,now())' % (create_userid,groupid)
	insert_result=sql_dml(sql)
	#添加完成员把is_empty设置为1
	sql='update `group` set is_empty=1 where id=%s' % groupid
	sql_dml(sql)
	if create_group_result:
		send_data(sock,u'【系统消息】群[ %s|GID:%s ]已经创建完成!' % (get_custom_time_string(),groupname,groupid))
	else:
		send_data(sock,u'【系统消息】群[ %s ]已经创建失败!' % (get_custom_time_string(),groupname))

def kickout_group_user(sock,args_list):
	if len(args_list)!=2:
		send_data(sock,u'【系统提示】踢群用户操作参数错误!')
		return	
	req_userid=get_userid_of_socket(sock)
	groupid=args_list[0]
	to_userid=args_list[1]
	#判断群是否存在
	if not groupid_is_exists(groupid):
		send_data(sock,"【系统提醒】该群GID[ %s ]不存在!" % groupid)
		return 
	own_userid=get_own_userid_of_groupid(groupid)
	own_username=get_username_of_userid(own_userid)
	groupname=get_groupname_of_groupid(groupid)

	#判断是否为群主,非群主不能踢用户
	if req_userid != own_userid:
		send_data(sock,u'【系统消息】 您不是群[ %s|GID:%s ]的群主，无权移出用户!' % (get_custom_time_string(),group_name,groupid))
		return

	if not userid_is_exists(to_userid):
		send_data(sock,u"【系统提示】用户UID[ %s ]不存在!" % to_userid)
		return

	to_username=get_username_of_userid(to_userid)
	#判断当前用户是否在群内
	if not userid_is_exists_in_group(to_userid):
		send_data(sock,u"【系统提示】用户[ %s|UID:%s ]不在群[ %s|GID: ]内!" % (to_username,to_userid,groupname,groupid))
		return

	###开始清理redis###
	r_key=KV_USERID_IN_GROUPID % (to_userid,groupid)
	r_redis.delete(r_key)
	r_key=LIST_USERIDS_OF_GROUPID % groupid
	r_redis.delete(r_key)
	###结束清理redis###

	#提醒群用户
	group_userids_list=get_userids_of_group(groupid)	
	#踢出用户
	sql='delete from group_users where groupid=%s and userid=%s' % (groupid,to_userid)
	delete_result=sql_dml(sql)
	if not delete_result:
		send_data(sock,u"【系统提示】数据库发送异常，移出用户失败!")
		return
	for group_userid in group_userids_list:
		if group_userid == to_userid:
			notice_content='[ %s ]\n【群消息】您已被群主[ %s|UID:%s ]移出群[ %s|GID:%s ]!' % (get_custom_time_string(),own_username,groupname,groupid)
		else:
			notice_content='[ %s ]\n【群消息】用户[ %s|UID:%s ]已被群主[ %s|UID:%s ]移出群[ %s|GID:%s ]!' % (get_custom_time_string(),to_username,to_userid,own_username,own_userid,groupname,groupid)
		#判断用户是否在线
		if user_is_online(group_userid):
			group_userid_sock=get_socket_of_userid(group_userid)
			send_data(group_userid_sock,notice_content)
		else:
			notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (group_userid,notice_content)
			sql_dml(notice_sql)

def delete_group(sock,args_list):
	if len(args_list)!=1:
		send_data(sock,u'【系统提醒】删群的操作参数错误')
		return	
	req_userid=get_userid_of_socket(sock)
	groupid=args_list[0]
	#判断群是否存在
	if not groupid_is_exists(groupid):
		send_data(sock,"【系统提醒】该群GID[ %s ]不存在!" % groupid)
		return 
	own_userid=get_own_userid_of_groupid(groupid)
	own_username=get_username_of_userid(own_userid)
	groupname=get_groupname_of_groupid(groupid)

	#判断是否为群主,非群主不能删群
	if req_userid != own_userid:
		send_data(sock,u'[ %s ]\n【系统提示】 您不是群[ %s|GID:%s ]的群主，无法删群!' % (get_custom_time_string(),groupname,groupid))
		return

	#获取群用户
	group_userids_list=get_userids_of_group(groupid)	
	
	#删除群用户
	sql='delete from group_users where groupid=%s ' % groupid
	delete_group_users_result=sql_dml(sql)
	if not delete_group_users_result:
		send_data(sock,u"【系统提示】数据库发送异常，移出用户失败!")
		return

	#删群
	sql='delete from `group` where id=%s ' % groupid 
	delete_group_result=sql_dml(sql)
	if not delete_group_result:
		send_data(sock,u"【系统提示】数据库发送异常，删除群失败!")
		return

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
	for group_userid in group_userids_list:
		r_key=KV_USERID_IN_GROUPID % (group_userid,groupid)
		r_redis.delete(r_key)
		r_key=LIST_GROUPMESSAGES_OF_USERID_IN_GROUPID % (groupid,group_userid)
		r_redis.delete(r_key)
	###结束更新redis###

	#获取群用户，发送删群提示
	for group_userid in group_userids_list:
		send_data(sock,'[ %s ]\n【系统消息】 群[ %s|GID:%s ]已解散!' % (get_custom_time_string(),groupname,groupid))
		if user_is_online(group_userid):
			to_sock=get_socket_of_userid(group_userid)
			send_data(to_sock,notice_content)
		else:
			notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (group_userid,notice_content)
			sql_dml(notice_sql)
	

def nospeak_group_user(sock,args_list,is_nospeak_group_user=1):
	if len(args_list)!=2:
		send_data(sock,u'【系统提示】用户操作参数错误!')
		return	
	req_userid=get_userid_of_socket(sock)
	groupid=args_list[0]
	to_userid=args_list[1]
	to_username=get_username_of_userid(to_userid)

	#判断群是否存在
	if not groupid_is_exists(groupid):
		send_data(sock,"【系统提醒】该群GID[ %s ]不存在!" % groupid)
		return 
	own_userid=get_own_userid_of_groupid(groupid)
	own_username=get_username_of_userid(own_userid)
	groupname=get_groupname_of_groupid(groupid)

	#判断是否为群主,非群主不能踢用户
	if req_userid != own_userid:
		send_data(sock,u'【系统消息】 您不是群[ %s|GID:%s ]的群主，无权禁言用户!' % (groupname,groupid))
		return

	if not userid_is_exists(to_userid):
		send_data(sock,u'【系统消息】 用户UID[ %s ]在系统中不存在' % (get_custom_time_string(),to_userid))
		return

	#判断当前用户是否在群内
	if not userid_is_exists_in_group(to_userid):
		send_data(sock,u"【系统提示】用户[ %s|UID:%s ]不在群[ %s|GID: ]内!" % (to_username,to_userid,groupname,groupid))
		return

	#判断当前用户是否在群内,并且是否禁用
	if user_is_forbidden_speaking_in_group(userid,groupid) and is_nospeak_group_user==1:
		send_data(sock,u"【系统消息】用户 [ %s|UID:%s ]在群[ %s|GID:%s ]内已被禁言，无需再次操作!" % (to_username,to_userid,groupname,groupid))
		return

	if not user_is_forbidden_speaking_in_group(userid,groupid) and is_nospeak_group_user==0:
		send_data(sock,u"【系统消息】用户 [ %s|UID:%s ]在群[ %s|GID:%s ]内未被禁言，无需此操作!" % (to_username,to_userid,groupname,groupid))
		return

	#禁言用户
	sql='update group_users set is_forbidden_speaking=%s where groupid=%s and userid=%s' % (is_nospeak_group_user,groupid,to_userid)
	result=sql_dml(sql)
	if not result:
		send_data(sock,u"【系统提示】数据库发送异常，用户操作失败!")
		return

	###开始清理redis###
	r_key=KV_IS_FORBIDDEN_SPEAKING_USERID_IN_GROUPID % (groupid,to_userid)
	r_redis.delete(r_key)
	###结束清理redis###

	#提醒群用户
	for group_userid in get_userids_of_group(groupid):
		if group_userid == to_userid:
			if is_nospeak_group_user == 1:
				notice_content='[ %s ]\n【系统消息】您已被群[ %s|GID:%s ]的群主[ %s|UID:%s ]禁言!' % (get_custom_time_string(),groupname,groupid,own_username,own_userid)
			else:
				notice_content='[ %s ]\n【系统消息】您已被群[ %s|GID:%s ]的群主[ %s|UID:%s ]取消禁言!' % (get_custom_time_string(),groupname,groupid,own_username,own_userid)
		else:
			if is_nospeak_group_user == 1:
				notice_content='[ %s ]\n【系统消息】用户[ %s|UID:%s ]已被群[ %s|GID:%s ]的群主[ %s|UID:%s ]禁言!' % (get_custom_time_string(),to_username,to_userid,groupname,groupid,own_username,own_userid)
			else:
				notice_content='[ %s ]\n【系统消息】用户[ %s|UID:%s ]已被群[ %s|GID:%s ]的群主[ %s|UID:%s ]取消禁言!' % (get_custom_time_string(),to_username,to_userid,groupname,groupid,own_username,own_userid)
		#判断用户是否在线
		if user_is_online(group_userid):
			notice_sock=get_socket_of_userid(group_userid)
			send_data(notice_sock,notice_content)
		else:
			notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (group_userid,notice_content)
			sql_dml(notice_sql)

def speak_group_user(sock,args_list):
	nospeak_group_user(sock,args_list,0)
	
def nospeak_group(sock,args_list):
	if len(args_list)!=1:
		send_data(sock,u'【系统提示】取消禁言用户操作参数错误!')
		return	
	req_userid=get_userid_of_socket(sock)
	groupid=args_list[0]

	#判断群是否存在
	if not groupid_is_exists(groupid):
		send_data(sock,"【系统提醒】该群GID[ %s ]不存在!" % groupid)
		return 
	own_userid=get_own_userid_of_groupid(groupid)
	own_username=get_username_of_userid(own_userid)
	groupname=get_groupname_of_groupid(groupid)

	#判断是否为群主,非群主不能踢用户
	if req_userid != own_userid:
		send_data(sock,u'【系统消息】 您不是群[ %s|GID:%s ]的群主，无权取消禁言用户!' % (groupname,groupid))
		return


	#判断当前群是否被禁言
	if is_group_forbidden_speaking:
		send_data(sock,u"【系统消息】群[ %s|ID:%s ]内已被禁言，无需再次操作!" % (groupname,groupid))
		return

	###开始清理redis###
	r_key=KV_IS_FORBIDDEN_SPEAKING_GROUPID % groupid
	r_redis.delete(r_key)
	###结束清理redis###

	#提醒群用户
	sql='select userid from group_users where groupid=%s' % groupid
	res=sql_query(sql)
	for r in res:
		notice_userid=r[0]
		notice_content='[ %s ]\n【系统消息】群[ %s|ID:%s ]已被群主[ %s ]禁言!' % (get_custom_time_string(),groupname,groupid,own_username)
		#判断用户是否在线
		if user_is_online(notice_userid):
			notice_sock=get_socket_of_userid(notice_userid)
			send_data(notice_sock,notice_content)
		else:
			notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (notice_userid,notice_content)
			sql_dml(notice_sql)

	#群禁言
	sql='update `group` set is_group_forbidden_speaking=1 where id=%s' % groupid
	sql_dml(sql)

def speak_group(sock,args_list):
	req_userid=get_userid_of_socket(sock)
	if len(args_list)!=1:
		send_data(sock,u'【系统提示】取消禁言群操作参数错误!')
		return	
	groupid=args_list[0]

	#判断群是否存在
	sql='select own_userid,name,is_group_forbidden_speaking from `group` where id=%s' % groupid 
	res=sql_query(sql)
	if not res:
		send_data(sock,"【系统提示】群ID号[ %s ]不存在!" % groupid)
		return 

	own_userid=res[0][0]
	own_username=get_username_of_userid(own_userid)
	groupname=res[0][1]
	is_group_forbidden_speaking=res[0][2]

	#判断是否为群主,非群主不能操作
	if req_userid != own_userid:
		send_data(sock,u'【系统消息】 您不是群[ %s|ID:%s ]的群主，无权取消群禁言!' % (groupname,groupid))
		return

	#判断当前群是否被禁言
	if not is_group_forbidden_speaking:
		send_data(sock,u"【系统消息】群[ %s|ID:%s ]内未被禁言，无需此操作!" % (groupname,groupid))
		return

	###开始清理redis###
	r_key=KV_IS_FORBIDDEN_SPEAKING_GROUPID % groupid
	r_redis.delete(r_key)
	###结束清理redis###

	#提醒群用户
	sql='select userid from group_users where groupid=%s' % groupid
	res=sql_query(sql)
	for r in res:
		notice_userid=r[0]
		notice_content='[ %s ]\n【系统消息】群[ %s|ID:%s ]已被群主[ %s ]取消禁言!' % (get_custom_time_string(),groupname,groupid,own_username)
		#判断用户是否在线
		if user_is_online(notice_userid):
			notice_sock=get_socket_of_userid(notice_userid)
			send_data(notice_sock,notice_content)
		else:
			notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (notice_userid,notice_content)
			sql_dml(notice_sql)

	#取消群禁言
	sql='update `group` set is_group_forbidden_speaking=0 where id=%s' % groupid
	sql_dml(sql)

def close_clear_all(sock,fd):
	print("清理客户端中...")
	try:
		epoll.unregister(fd)
		#从命令统计列表删除
		del fd_to_cmd_count[fd]
		#从在线userid2sock删除
		del_online_sock(sock)
		#从queue队列中删除
		del fd_to_message_queue[fd]
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
			print(u'[ %s ] 获取客户端数据异常' % get_custom_time_string())
			err_msg=u'echo 数据包异常'
			fd_to_message_queue[fd].put(err_msg)
			#获取客户端数据异常，猜测客户端非正常关闭
			epoll.modify(fd,0)
			try:
				fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
			except:
				close_clear_all(c_sock,fd)
				
			return	
		#判断包头字段里填入的长度和实际收到的是否一致	
		if len(data)!=length:
			print(u'[ %s ] 客户端数据包长度异常' % get_custom_time_string())
			err_msg=u'echo 数据包长度异常'
			fd_to_message_queue[fd].put(err_msg)
			return

		#判断命令是否为auth,是，则进行认证，否则跳转到EPOLLOUT
		else:
			cmd=data.decode('utf-8').split(' ')[0]
			#进行认证
			if cmd == 'auth':
				#获取用户名和密码
				try:
					userid=data.decode('utf-8').split(' ')[1]
					pwd=data.decode('utf-8').split(' ')[2]
				except:
					try:
						epoll.modify(fd,select.EPOLLOUT)	
					except:
						epoll.modify(fd,0)
						fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
					return

			
				#进行认证
				if user_auth(userid,pwd):
					#注销同一账号其他客户端的连接,实现单点登录
					try:
						#获取用户id
						userid=user_name_userid(user)	
						#获取用户socket
						user_sock=get_socket_of_userid(userid)
						#对用户发送sock请求
						logout_msg=u'echo 该账号在其他地方登录，您被迫已下线!'
						fd_to_message_queue[user_sock.fileno()].put(logout_msg)
						try:
							epoll.modify(user_sock.fileno(),select.EPOLLOUT)	
							#清除认证用户列表
							del_online_sock(user_sock)
							#清除登录列表的记录
							if user_sock.fileno() in auth_fd_to_socket.keys():
								del auth_fd_to_socket[user_sock.fileno()]
						except:
							epoll.modify(user_sock.fileno(),0)
							fd_to_socket[user_sock.fileno()].shutdown(socket.SHUT_RDWR)
					except:
						pass
					#添加到在线用户列表
					userid_to_socket[userid]=c_sock
					#添加到在线fd2sock列表
					auth_fd_to_socket[fd]=c_sock
					#提示登录成功的信息

					#加入用户sock2command_count
					fd_to_cmd_count[fd]=0
					login_ok=u'echo 登录成功!'
					fd_to_message_queue[fd].put(login_ok)
					try:
						print("[ %s ]开始修改epollout!" % time.ctime())
						epoll.modify(fd,select.EPOLLOUT)	
					except:
						epoll.modify(fd,0)
						fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
					return
				else:
					login_fail=u'echo 登录失败，密码错误!'
					fd_to_message_queue[fd].put(login_fail)
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
			print(u'[ %s ] 获取客户端数据异常' % get_custom_time_string())
			err_msg=u'echo 数据包异常'
			fd_to_message_queue[fd].put(err_msg)
			epoll.modify(fd,0)
			fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
			return
		#判断包头字段里填入的长度和实际收到的是否一致	
		if len(data)!=length:
			print(u'[ %s ] 客户端数据包长度异常' % get_custom_time_string())
			err_msg=u'echo 数据包异常'
			fd_to_message_queue[fd].put(err_msg)
			try:
				epoll.modify(fd,select.EPOLLHUP)
			except:
				epoll.modify(fd,0)
				fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
			return
		
		#把数据存入队列
		fd_to_message_queue[fd].put(data.decode('utf-8'))
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
			message=fd_to_message_queue[fd].get_nowait()
			cmd=message.split(' ')[0]
			#判断是否是服务器的信息
			if cmd == 'echo':
				echo_reply=message.split(' ')[1:]
				send_data(c_sock,' '.join(echo_reply))
			elif cmd == 'close':
				print(u'[ %s ] 客户端和服务器断开连接' % get_custom_time_string())
				send_data(c_sock,u'您已经与服务器断开连接')
				#关闭连接
				epoll.modify(fd,0)
				fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
				return
		except queue.Empty:
			prompt_auth_msg=u'请输入用户名密码登录!!!\n命令格式:auth 用户名 密码'
			send_data(c_sock,prompt_auth_msg)
		try:
			epoll.modify(fd,select.EPOLLIN)
		except:
			epoll.modify(fd,0)
			fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
		return
	#如果已经登录则推送，获取队列中的内容，判断消息的类别，调用不同的函数处理
	else:
		#推送消息
		if fd_to_cmd_count[fd]==0:
			push_userid=get_userid_of_socket(c_sock)
			if push_userid:
				push_messages(c_sock,push_userid)
		fd_to_cmd_count[fd]+=1
		#获取队列中的内容
		try:
			message=fd_to_message_queue[fd].get_nowait()
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
			send_data(c_sock,' '.join(echo_reply))
		elif cmd == 'help' or cmd == '?':
			send_data(c_sock,cmd_help)
		elif cmd == 'close':
			print(u'[ %s ] 客户端和服务器断开连接' % get_custom_time_string())
			send_data(c_sock,u'您已经与服务器断开连接')
			#关闭连接
			epoll.modify(fd,0)
			fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
			return
		elif cmd == 'logout':
			print(u'[ %s ] 退出系统' % get_custom_time_string())
			send_data(c_sock,u'退出系统!')
			#清除认证用户列表
			del_online_sock(c_sock)
			#重置执行命令的列表
			fd_to_cmd_count[fd]=0
			#清除登录列表的记录
			if fd in auth_fd_to_socket.keys():
				del auth_fd_to_socket[fd]
		elif cmd == 'auth':
			send_data(c_sock,u'【系统提示】提示!您已认证!')
		elif cmd not in noargs_cmd+args_cmd:
			print(u'[ %s ] 客户端的命令不支持,请使用 help/?' % get_custom_time_string())
			send_data(c_sock,u'【系统提示】输入的命令不支持,请使用 help/?')
		else:
			args=message.split(' ')[1:]
			#命令分发
			switch={
				'msg': send_user_message,
				'm': send_user_message,
				'gmsg': send_group_message,
				'gm': send_group_message,
				'addfriend': add_friend,
				'af': add_friend,
				'delfriend': delete_friend,
				'df': delete_friend,
				'entergroup': enter_group,
				'eng': enter_group,
				'exitgroup': exit_group,
				'exg': exit_group,
				'kickout': kickout_group_user,
				'ko': kickout_group_user,
				'reject': reject_requset,
				'rj': reject_requset,
				'accept': accept_request,
				'ac': accept_request,
				'listfriends': list_friends,
				'lf': list_friends,
				'listgroups': list_groups,
				'lg': list_groups,
				'listallgroup': list_all_groups,
				'lag': list_all_groups,
				'listgroupusers': list_group_users,
				'lgu': list_group_users,
				'creategroup': create_group,
				'cg': create_group,
				'delgroup': delete_group,
				'dg': delete_group,
				'nospkgroup': nospeak_group,
				'nsg': nospeak_group,
				'spkgroup': speak_group,
				'sg': speak_group,
				'nospkuser': nospeak_group_user,
				'nsu': nospeak_group_user,
				'spkuser': speak_group_user,
				'su': speak_group_user
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

		r_key=KV_IS_FORBIDDEN_SPEAKING_USERID_IN_GROUPID % (groupid,userid)
		r_redis.set(r_key,is_forbidden_speaking)

	#查询所有群ID,是否禁用，所有群成员ID列表
	sql='select id,is_group_forbidden_speaking from `group`'
	res=sql_query(sql)
	for r in res:
		gid=r[0]
		is_group_forbidden_speaking=int(r[1])
		r_key=KV_EXISTS_GROUPID % gid
		r_redis.set(r_key,"")
		
		r_key=KV_IS_FORBIDDEN_SPEAKING_GROUPID % gid
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
			print("启动聊天服务器%s:%s!!!" %(host,port))
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
							new_sock,addr=ssl_s.accept()
						except ssl.SSLError as e:
							print("套接字异常错误:",e)
							continue
						new_sock.setblocking(False)
						c_fd=new_sock.fileno()
						#注册到epoll中
						try:
							epoll.register(c_fd,select.EPOLLOUT)
						except:
							epoll.unregister(c_fd)
							new_sock.close()
							continue
						#添加到fd_to_socket字典表中
						fd_to_socket[c_fd]=new_sock
						fd_to_message_queue[c_fd]=queue.Queue()
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
