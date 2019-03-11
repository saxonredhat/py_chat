import sys
import socket
import time
import threading
import struct
import pymysql
import selectors
import select
import queue

#数据库配置
db_config={
	'host':'localhost',
	'port':3306,
	'user':'root',
	'passwd':'Sys20180808!',
	'db':'chat',
	'charset':'utf8'
}

#监听配置
host='0.0.0.0'
try:
	port=int(sys.argv[1])
except:
	port=6900

#无参数命令
noargs_cmd=['listfriends','lf','listgroup','lg','listallgroup','lag']

#参数命令
args_cmd=['msg','m','gmsg','gm','adduser','au','deluser','du','addgroup','ag','exitgroup','eg','reject','rj','accept','ac','creategroup','cg','delgroup','dg','listgroupusers','lgu']

#活动的客户端socket列表
userid_to_socket={}

#认证在线socket列表
auth_fd_to_socket={}

#在线socket列表
fd_to_socket={}

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
	except:
		#del_online_sock(conn)
		#sel.unregister(conn)
		conn.close()

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
	print("用户[%s]认证成功" % user)
	return True 

def get_data(conn):
	#获取2个字节数据,为数据的大小
	length=struct.unpack('H',conn.recv(2))[0]
	data=conn.recv(int(length))
	if len(data)!=length:
		print(u'[ %s ] 客户端数据包异常！' % (time.strftime("%Y-%m-%d %X")))
		data_err_msg=u'认证信息异常'
		send_msg(conn,data_err_msg)
		return
	return data

def user_conn_auth(conn,cli_ip,cli_port):
	#获取2个字节数据,为数据的大小
	length=struct.unpack('H',conn.recv(2))[0]
	data=conn.recv(int(length))
	if len(data)!=length:
		print(u'[ %s ] 客户端(%s:%s)认证异常' % (time.strftime("%Y-%m-%d %X"),cli_ip,cli_port))
		auth_err_msg=u'认证信息异常'
		send_msg(conn,auth_err_msg)
		return False
	else:
		#获取命令
		cmd=data.decode('utf-8').split(' ')[0]
		if cmd != 'auth':
			print(u'[ %s ] 客户端(%s:%s)认证命令格式错误:%s' % (time.strftime("%Y-%m-%d %X"),cli_ip,cli_port,cmd))
			auth_format_err_msg=u'认证命令格式错误！\n认证命令:auth username password'
			send_msg(conn,auth_format_err_msg)	
			return False
		else:
			user=msg.decode('utf-8').split(' ')[1]
			pwd=msg.decode('utf-8').split(' ')[2]
			sql="select id,username,password from user where username='%s' LIMIT 1" % user
			res=sql_query(sql)
			if not res:
				print(u'[ %s ] 客户端认证用户名%s不存在！' % (time.strftime("%Y-%m-%d %X"),user))
				auth_user_not_exist_err_msg=u'客户端认证用户名不存在!'
				send_msg(conn,auth_user_not_exist_err_msg)
				return False
			if res[0][2]!=pwd:
				print(u'[ %s ] 客户端(%s:%s)认证用户%s密码错误！' % (time.strftime("%Y-%m-%d %X"),user))
				auth_pwd_err_msg=u'用户名或者密码错误!'
				send_msg(conn,auth_pwd_err_msg)
				return False
			return True
			
def get_userid(username):
	sql='select id from user where username="%s" LIMIT 1' % username
	try:
		userid=sql_query(sql)[0][0]
	except:
		userid=-1
	return userid
			

def push_new_msg(conn,user):
	#获取用户id
	sql='select id from user where username="%s" LIMIT 1' % user
	print(sql)
	user_id=sql_query(sql)[0][0]
	
	#获取群id
	sql='select groupid from group_users where userid=%s' % user_id
	print(sql)
	group_ids_list=[]
	res=sql_query(sql)
	for r in res:
		group_ids_list.append(str(r[0]))
	group_ids=','.join(group_ids_list)

	#是否收到群消息
	if group_ids:
		sql='select id,content,groupid,from_userid from gmsg where groupid in (%s) and is_all_pushed=0' % group_ids
		print(sql)
		res=sql_query(sql)
		for r in res:
			msgid=r[0]
			content=r[1]
			groupid=r[2]
			from_userid=r[3]
			sql='select id from gmsg_push_record where id=%s and to_userid=%s LIMIT 1' % (msgid,user_id)
			print(sql)
			res=sql_query(sql)
			if not res[0][0]:
				sql='select name from group where id=%s' % groupid
				print(sql)
				res=sql_query(sql)
				groupname=res[0][0]
				sql='select username from user where id="%s"' % from_userid
				print(sql)
				res=sql_query(sql)
				sender=res[0][0]
				send_msg(conn,u"群[消息] %s -- %s :" % (groupname,sender,content))
				#添加群消息推送记录
				sql='insert into gmsg_push_record(gmsgid,to_userid,created_at) values(%s,%s,now())' % (msgid,user_id)
				print(sql)
				sql_dml(sql)
	
	#是否收到用户消息
	sql='select id,userid,content,created_at from msg where to_userid=%s and is_pushed=0' % user_id
	print(sql)
	res=sql_query(sql)
	if res:
		for r in res:
			rowid=r[0]
			userid=r[1]
			content=r[2]
			created_at=r[3]
			sql='select username from user where id=%s'% userid
			res=sql_query(sql)
			from_user=res[0][0]
			send_msg(conn,u'%s\n[%s]: %s ' % (created_at,from_user,content))
			#更新数据库记录
			sql='update msg set is_pushed=1 where id=%s' % rowid
			print(sql)
			sql_dml(sql)
	
	
	#是否收到加好友消息
	sql='select id,userid from user_req where add_userid=%s and status=0' % user_id 
	res=sql_query(sql)
	if res:
		for r in res:
			req_id=r[0]
			req_userid=r[1]
			req_username=user_userid_name(req_userid)
			send_msg(conn,u"用户%s向您申请添加好友请求!\n请求号为%s\n同意：accept %s \n拒绝：reject %s" % (req_username,req_id,req_id,req_id))

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
			send_msg(conn,u'用户[%s]申请加入群[%s | ID:%s]\n申请号:%s\n同意：accept %s \n拒绝：reject %s' % (req_username,group_name,groupid,req_id,req_id,req_id))
	
	#是否收到提醒消息
	sql='select id,content from user_notice where status=0 and userid=%s' % user_id  
	res=sql_query(sql)
	if res:
		for r in res:
			send_msg(conn,r[1])
			sql='update user_notice set status=1 where id=%s' % r[0]
			sql_dml(sql)
	

def user_is_online(userid):
	for uid,conn_user in userid_to_socket.items():
		if userid == uid:
			return True
	return False

def get_userid_conn(userid):
	for uid,conn_user in userid_to_socket.items():
		if userid == uid:
			return conn_user
	

def user_msg(conn,args):
	from_userid=get_conn_userid(conn)
	sql='select username from user where id=%s' % from_userid 
	res=sql_query(sql)
	if not res:
		print(u'[ %s ] 用户id[%s]在系统中不存在' % (time.strftime("%Y-%m-%d %X"),from_userid))	
		send_msg(conn,u'[ 系统消息 ] 用户id[%s]在系统中不存在' % from_userid)
	from_username=res[0][0]
	if from_userid < 0:
		return 
	to_username=args[0]
	content=' '.join(args[1:])
	sql='select id from user where username="%s"' % to_username
	res=sql_query(sql)
	if not res:
		print(u'[ %s ] 用户%s在系统中不存在' % (time.strftime("%Y-%m-%d %X"),to_username))	
		send_msg(conn,u'[ 系统消息 ] 用户%s在系统中不存在' % to_username)
		return	
	to_userid=res[0][0]
	#判断是否为自己发送给自己
	if from_userid == to_userid:
		user_msg=u'%s\n[ %s ]: %s' % (time.strftime("%Y-%m-%d %X"),from_username,content)
		send_msg(conn,user_msg)
		return
	#判断当前发送消息的接收方是否为好友，如果不是拒绝发送消息
	sql='select userid from user_users where userid=%s and friend_userid=%s' % (from_userid,to_userid)
	res=sql_query(sql)
	if not res:
		send_msg(conn,u'该用户还不是您的好友！不能发送消息')
		return 
	to_conn=get_userid_conn(to_userid)
	user_msg=u'%s\n[ %s ]: %s' % (time.strftime("%Y-%m-%d %X"),from_username,content)
	if to_conn:
		try:
			send_msg(to_conn,user_msg)
		except:
			sql='insert into msg(userid,to_userid,content,is_pushed) values(%s,%s,"%s",0)' % (from_userid,to_userid,content)
			sql_dml(sql)
	else:
		sql='insert into msg(userid,to_userid,content,is_pushed) values(%s,%s,"%s",0)' % (from_userid,to_userid,content)
		sql_dml(sql)
	send_msg(conn,user_msg)

def user_gmsg(conn,args):
	req_userid=get_conn_userid(conn)
	req_username=user_userid_name(req_userid)
	args_list=args
	if len(args_list)<2:
		send_msg(conn,u'发群消息的操作参数错误')
		return	
	groupid=args_list[0]
	content=' '.join(args_list[1:])

	#判断群是否存在
	sql='select id,own_userid,name from `group` where id=%s' % groupid 
	res=sql_query(sql)
	if not res:
		send_msg(conn,"该群ID号[%s]不存在！" % groupid)
		return 
	own_userid=res[0][1]
	group_name=res[0][2]

	#判断是否为群成员，非成员不能发送消息
	sql='select id from group_users where groupid=%s and userid=%s' % (groupid,req_userid)
	res=sql_query(sql)
	if not res:
		send_msg(conn,"您非群[%s |ID:%s]成员，不能发送群信息！" % (group_name,groupid))
		return

	#获取群用户，发送群消息
	sql='select userid from group_users where groupid=%s' % (groupid)
	res=sql_query(sql)
	if res:
		for r in res:
			to_userid=r[0]
			notice_content='%s\n[群消息][%s |ID:%s][%s]:%s' % (time.strftime("%Y-%m-%d %X"),group_name,groupid,req_username,content)
			#判断用户是否在线
			if user_is_online(to_userid):
				to_conn=get_userid_conn(to_userid)
				send_msg(to_conn,notice_content)
			else:
				notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (to_userid,notice_content)
				sql_dml(notice_sql)

def handle_new_conn(conn,cli_ip,cli_port):
	pass

def user_userid_name(userid):
	sql='select username from user where id=%s' % userid
	res=sql_query(sql)
	if not res:
		return ''
	return res[0][0]
	

def user_adduser(conn,args):
	userid=get_conn_userid(conn)	
	req_username=user_userid_name(userid)
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'添加好友的命令错误')
		return
	to_username=args_list[0]
	sql='select id from user where username="%s"' % to_username
	res=sql_query(sql)
	if not res:
		send_msg(conn,u"系统不存在用户%s" % to_username)
		return
	add_userid=res[0][0]

	#判断是否添加的是自己
	if userid == add_userid:
		send_msg(conn,u"不能添加自己为好友！")
		return 1 

	#判断是否已经添加为好友,或者是否已经存在申请请求
	sql='select id from user_users where userid=%s and friend_userid=%s' % (userid,add_userid)
	res=sql_query(sql)
	if res:
		send_msg(conn,u"您和用户%s已经是好友关系，不需要再次添加" % to_username)
		return
	sql='select id from user_req where userid=%s and add_userid=%s and type=1 and status=0' % (userid,add_userid)
	res=sql_query(sql)
	if res:
		send_msg(conn,u"您已经向%s发送过好友申请，无需再次发送" % to_username)
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
		send_msg(to_conn,u"用户%s向您申请添加好友请求!\n请求号为%s\n同意：accept %s \n拒绝：reject %s" % (req_username,req_id,req_id,req_id))
	send_msg(conn,u'已向该用户%s发送添加好友请求' % to_username)

def user_deluser(conn,args):
	userid=get_conn_userid(conn)	
	req_username=user_userid_name(userid)
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'删除好友的命令错误')
		return
	to_username=args_list[0]
	sql='select id from user where username="%s"' % to_username
	res=sql_query(sql)
	if not res:
		send_msg(conn,u"系统不存在用户[%s]" % to_username)
		return
	del_userid=res[0][0]
	#判断是否已经是好友
	sql='select id from user_users where userid=%s and friend_userid=%s' % (userid,del_userid)
	res=sql_query(sql)
	if not res:
		send_msg(conn,u"您和用户[%s]不是好友关系" % to_username)
		return
	#双向解除好友关系
	sql='delete from user_users where userid=%s and friend_userid=%s' % (userid,del_userid) 
	sql_dml(sql)
	sql='delete from user_users where userid=%s and friend_userid=%s' % (del_userid,userid) 
	sql_dml(sql)
	send_msg(conn,u"您和用户[%s]已经解除好友关系" % to_username)
	
	#通知对方
	to_conn=get_userid_conn(del_userid)
	notice_content='用户[%s]已和你您解除好友关系' % req_username
	if to_conn:
		send_msg(to_conn,notice_content)
	else:
		notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (del_userid,notice_content)
		sql_dml(notice_sql)
	

def user_addgroup(conn,args):
	req_userid=get_conn_userid(conn)
	#判断参数是否正确
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'加群的操作参数错误')
		return

	groupid=args_list[0]
	#判断群是否存在
	sql='select id,own_userid,name from `group` where id=%s' % groupid 
	res=sql_query(sql)
	if not res:
		send_msg(conn,"该群ID号[%s]不存在！" % groupid)
		return 
	own_userid=res[0][1]
	group_name=res[0][2]
	
	#判断用户是否已经加入
	sql='select g.id,g.name from `group` g left join group_users gu on gu.groupid=g.id where g.id=%s and (g.own_userid=%s or gu.userid=%s)' % (groupid,req_userid,req_userid)
	res=sql_query(sql)
	if res:
		send_msg(conn,u'您已经在群[%s | ID:%s]中,无需加入！'% (res[0][1],res[0][0]))
		return

	#判断是否已经存在加入该群的申请请求
	sql='select ur.id,g.id,g.name from user_req ur left join `group` g on ur.add_groupid=g.id where ur.userid=%s and ur.add_groupid=%s and ur.status=0' % (req_userid,groupid)
	res=sql_query(sql)
	if res:
		send_msg(conn,u'您已经申请过加入群[%s | ID:%s]，请等待回复！'% (res[0][2],res[0][1]))
		return
	
	#添加加群请求
	sql='insert into user_req(userid,type,add_groupid,status,created_at) values(%s,2,%s,0,now())' % (req_userid,groupid)
	sql_dml(sql)
	req_username=user_userid_name(req_userid)

	send_msg(conn,u'您申请了加入群[%s | ID:%s]，请等待回复！'% (group_name,groupid))
	
	sql='select id from user_req where userid=%s and add_groupid=%s and status=0 LIMIT 1' % (req_userid,groupid)
	res=sql_query(sql)
	if not res:
		return
	req_id=res[0][0]
	#判断群主是否在在线，在线直接发送加群提醒，如果不在线，群主通过user_req可以看到加群消息	
	if user_is_online(own_userid):
		own_conn=get_userid_conn(own_userid)
		send_msg(own_conn,u'用户[%s]申请加入群%s群[ID号:%s]\n申请号:%s\n同意：accept %s \n拒绝：reject %s' % (req_username,group_name,groupid,req_id,req_id,req_id))



def user_exitgroup(conn,args):
	req_userid=get_conn_userid(conn)
	req_username=user_userid_name(req_userid)
	#判断参数是否正确
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'退群的操作参数错误')
		return

	groupid=args_list[0]
	#判断群是否存在
	sql='select id,own_userid,name from `group` where id=%s' % groupid 
	res=sql_query(sql)
	if not res:
		send_msg(conn,"该群ID号[%s]不存在！" % groupid)
		return 
	own_userid=res[0][1]
	group_name=res[0][2]
	
	#判断用户是否已经加入
	sql='select g.id,g.name from `group` g left join group_users gu on gu.groupid=g.id where g.id=%s and (g.own_userid=%s or gu.userid=%s)' % (groupid,req_userid,req_userid)
	res=sql_query(sql)
	if not res:
		send_msg(conn,u'您不在群[%s | ID:%s]中,无需退出！'% (res[0][1],res[0][0]))
		return

	#判断是否为群主,群主无法退群
	if req_userid == own_userid:
		send_msg(conn,u'您是群[%s |ID:%s]的群主，无法退群！' % (group_name,groupid))
		return

	#退群请求
	sql='delete from group_users where userid=%s and groupid=%s' % (req_userid,groupid)
	sql_dml(sql)
	send_msg(conn,u'您已退出群[%s |ID:%s]！' % (group_name,groupid))
	#判断群主是否在在线，在线直接发送退群提醒，如果不在线，把退群消息加入到user_notice	
	notice_content=u'用户[%s]退出群[%s |ID号:%s]' % (req_username,group_name,groupid)
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
		send_msg(conn,u'同意请求的操作参数错误')
		return	
	req_id=args_list[0]
	sql='select id,userid,add_userid,add_groupid from user_req where id=%s and status=0' % req_id
	res=sql_query(sql)
	if not res:
		send_msg(conn,u'请求的ID号不存在')
		return
	req_userid=res[0][1]
	add_userid=res[0][2]
	add_groupid=res[0][3]
	#判断是加群还是添加好友 
	if add_userid:
		#判断当前用户是否有权限执行
		if cur_userid != add_userid:
			send_msg(conn,u'您无权执行该操作！')
			return 1
			
		#互加好友操作
		sql='insert into user_users(userid,friend_userid,created_at) values(%s,%s,now())' % (req_userid,add_userid)
		sql_dml(sql)
		sql='insert into user_users(userid,friend_userid,created_at) values(%s,%s,now())' % (add_userid,req_userid)
		sql_dml(sql)
	
		#消息提醒内容
		notice_content=u'用户%s已经同意添加您为好友!' % user_userid_name(add_userid)
		send_msg(conn,u'您已同意添加%s为好友' % user_userid_name(add_userid))

	if add_groupid:
		#判断当前用户是否是群主,非群主不能执行该操作
		sql='select own_userid from `group` where id=%s' % add_groupid
		res=sql_query(sql)
		if not res:
			send_msg(conn,u'遇到UF0！')
			return
		own_userid=res[0][0]
		if cur_userid != own_userid:
			send_msg(conn,u'您非该群的群主，无权执行该操作！')
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
			notice_content=u"[群主:%s]已经同意你加入群[%s | ID:%s]！" % (user_userid_name(group_own_userid),group_name,groupid)
			send_msg(conn,u'您已同意用户[%s]加入群[%s | ID:%s]！' %(user_userid_name(req_userid),group_name,groupid) )

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
		send_msg(conn,u'拒绝请求的操作参数错误')
		return	
	req_id=args_list[0]
	sql='select id,userid,add_userid,add_groupid from user_req where id=%s and status=0' % req_id
	res=sql_query(sql)
	if not res:
		send_msg(conn,u'请求的ID号不存在')
		return
	req_userid=res[0][1]
	add_userid=res[0][2]
	add_groupid=res[0][3]

	#如果是添加好友 
	if add_userid:
		#判断当前用户是否有权限执行
		if cur_userid != add_userid:
			send_msg(conn,u'您无权执行该操作！')
			return 1
		notice_content=u'用户%s已经婉拒了您添加好友的请求！' % user_userid_name(add_userid)
		send_msg(conn,u'您已拒绝用户%s的添加好友申请' % user_userid_name(add_userid))

	#如果是添加群
	if add_groupid:
		#判断当前用户是否是群主,非群主不能执行该操作
		sql='select own_userid from `group` where id=%s' % add_groupid
		res=sql_query(sql)
		if not res:
			send_msg(conn,u'遇到UF0！')
			return
		own_userid=res[0][0]
		if cur_userid != own_userid:
			send_msg(conn,u'您非该群的群主，无权执行该操作！')
			return 2 
		#获取群的信息
		sql='select id,name,own_userid from `group` where id=%s' % add_groupid
		res=sql_query(sql)
		if res:
			groupid=res[0][0]
			group_name=res[0][1]
			group_own_userid=res[0][2]	
			notice_content=u"[群主:%s]已经拒绝你加入群[%s | ID:%s]的请求！" % (user_userid_name(group_own_userid),group_name,groupid)
			send_msg(conn,u'您已拒绝用户[%s]加入群[%s | ID:%s]的请求！' %(user_userid_name(req_userid),group_name,groupid) )

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
		send_msg(conn,u'查看群的操作参数错误')
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
		send_msg(conn,u'查看群的操作参数错误')
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
		send_msg(conn,u'列出群成员的操作参数错误')
		return	
	groupid=args_list[0]

	#判断群是否存在
	sql='select id,own_userid,name from `group` where id=%s' % groupid 
	res=sql_query(sql)
	if not res:
		send_msg(conn,"该群ID号[%s]不存在！" % groupid)
		return 
	own_userid=res[0][1]
	group_name=res[0][2]

	#判断是否为群成员，非成员不能查看群信息
	sql='select id from group_users where groupid=%s and userid=%s' % (groupid,req_userid)
	res=sql_query(sql)
	if not res:
		send_msg(conn,"您非群[%s |ID:%s]成员，不能查看群成员信息！" % (group_name,groupid))
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
		send_msg(conn,'群[%s |ID:%s]成员信息\n%s' %(group_name,groupid,'\n'.join(mem_list)))
		

def user_creategroup(conn,args):
	own_userid=get_conn_userid(conn)
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'创建群的操作参数错误')
		return	
	group_name=args_list[0]

	#判断该用户的群是否已经达到上限
	sql='select groups_limit from user where id=%s' % own_userid
	res=sql_query(sql)
	if not res:
		send_msg(conn,u'查看数据库异常')
		return 
	groups_limit=res[0][0]
	if groups_limit == 0:
		send_msg(conn,u'您创建的群数量已达上限数为%s' % groups_limit)
		return
	sql='select count(1) from `group` where own_userid=%s' % own_userid
	res=sql_query(sql)
	if not res:
		send_msg(conn,u'查看数据库异常')
		return
	group_count=res[0][0]
	if group_count+1>groups_limit:
		send_msg(conn,u'您创建的群数量已达上限数为%s,当前拥有群数为%s' % (groups_limit,group_count))
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
	
	send_msg(conn,u'群[%s]已经创建完成！' % group_name)

def user_delgroup(conn,args):
	req_userid=get_conn_userid(conn)
	args_list=args
	if len(args_list)!=1:
		send_msg(conn,u'删群的操作参数错误')
		return	
	groupid=args_list[0]

	#判断群是否存在
	sql='select id,own_userid,name from `group` where id=%s' % groupid 
	res=sql_query(sql)
	if not res:
		send_msg(conn,"该群ID号[%s]不存在！" % groupid)
		return 
	own_userid=res[0][1]
	group_name=res[0][2]

	#判断是否为群主,非群主不能删群
	if req_userid != own_userid:
		send_msg(conn,u'您不是群[%s |ID:%s]的群主，无法删群！' % (group_name,groupid))
		return

	#获取群用户，发送删群提示
	sql='select userid from group_users where groupid=%s and userid!=%s' % (groupid,own_userid)
	res=sql_query(sql)
	if res:
		for r in res:
			to_userid=r[0]
			notice_content='[系统消息] 群主已解散群[%s |ID:%s]！' % (group_name,groupid)
			#判断用户是否在线
			if user_is_online(to_userid):
				to_conn=get_userid_conn(to_userid)
				send_msg(to_conn,notice_content)
			else:
				notice_sql='insert into user_notice(userid,content,status,created_at) values(%s,"%s",0,now())' % (to_userid,notice_content)
				sql_dml(notice_sql)
			
	#删除群用户
	sql='delete from group_users where groupid=%s ' % groupid
	sql_dml(sql)

	#删群
	sql='delete from `group` where id=%s ' % groupid 
	sql_dml(sql)

	send_msg(conn,'[系统消息] 群[%s |ID:%s]已解散！' % (group_name,groupid))


def single_login(user):
	try:
		#获取用户id
		userid=user_name_userid(user)	
		#获取用户sock
		conn=get_userid_conn(userid)
		#对用户发送sock请求
		send_msg(conn,'该账号在其他地方登录，您被迫已下线！')
		fd=conn.fileno()
		epoll.unregister(conn)
		#删除这个用户的队列
		del userid_to_socket[userid] 
		#删除fd2sock_在线列表
		del auth_fd_to_socket[fd]
		#删除这个用户的连接
		del message_queue[fd]
		conn.close()	
	except:
		pass

def accept(sock,mask):
	conn,addr=sock.accept()
	print(u'[ %s ] 新客户端连接 -- %s:%s' % (time.strftime("%Y-%m-%d %X"),addr[0],addr[1]))
	#对用户发送提示语,提示他进行认证
	prompt_auth_msg=u'欢迎，请输入用户名，密码进行登录!\n登录命令格式:auth username password'
	send_msg(conn,prompt_auth_msg)
	while True:
		#获取用户命令信息
		try:
			data=get_data(conn)
		except:
			conn.close()
			break
			
		try:
			cmd=data.decode('utf-8').split(' ')[0]
			if cmd !='auth':
				print(u'客户端应该输入认证命令')
				login_msg=u'请登录，才能操作!\n登录命令格式:auth username password'
				send_msg(conn,login_msg)
				continue	
			user=data.decode('utf-8').split(' ')[1]	
			pwd=data.decode('utf-8').split(' ')[2]
			
		except:
			login_err_msg=u'登录命令格式不正确！\n登录命令格式:auth username password'
			send_msg(conn,login_err_msg)
			print(login_err_msg)
			continue

		#获取用户userid
		userid=user_name_userid(user)
		if userid < 0:
			login_err_msg=u'用户名不存在！'
			send_msg(conn,login_err_msg)
			print(login_err_msg)
			continue
	
		#进行认证，如果认证成功放入到selector
		if user_auth(user,pwd):
			#注销其他客户端的连接
			single_login(user)
			#提示登录成功的信息
			success_msg=u'登录成功!'
			send_msg(conn,success_msg)
			#推送消息
			push_new_msg(conn,user)
			#添加到在线用户列表
			userid_to_socket[userid]=conn
			#注册到selectors
			sel.register(conn,selectors.EVENT_READ,read)
			break
		else:
			login_err_msg=u'登录失败，密码错误！'
			send_msg(conn,login_err_msg)
			print(login_err_msg)

def read(conn,mask):
	try:
		length=struct.unpack('H',conn.recv(2))[0]
		data=conn.recv(int(length))
	except:
		try:
			del_online_sock(conn)
			sel.unregister(conn)
			conn.close()	
			return 1
		except:
			print(u'客户端异常关闭')
			return 2

	print("length:%s data:%s" % (length,data.decode('utf-8')))
	if len(data)!=length:
		print(u'[ %s ] 客户端认证异常' % time.strftime("%Y-%m-%d %X"))
		pkt_err_msg=u'数据包异常'
		send_msg(conn,pkt_err_msg)
		return 1
	cmd=data.decode('utf-8').split(' ')[0]
	if cmd == 'close':
		print(u'[ %s ] 客户端与服务器端口连接' % time.strftime("%Y-%m-%d %X"))
		send_msg(conn,u'您已经与服务器端口连接')
		#从在线列表断开
		del_online_sock(conn)
		#从selector删除
		sel.unregister(conn)
		#关闭当前conn.close()
		conn.close()
		return 2
	if cmd == 'auth':
		send_msg(conn,u'提示!您已认证！')
		return 3
	if cmd not in noargs_cmd+args_cmd:
		print(u'[ %s ] 客户端的命令不支持' % time.strftime("%Y-%m-%d %X"))
		send_msg(conn,u'输入的命令服务器不支持')
		return 3
	args=data.decode('utf-8').split(' ')[1:]
	switch={
		'msg': user_msg,
		'm': user_msg,
		'gmsg': user_gmsg,
		'gm': user_gmsg,
		'adduser': user_adduser,
		'au': user_adduser,
		'deluser': user_deluser,
		'du': user_deluser,
		'addgroup': user_addgroup,
		'ag': user_addgroup,
		'exitgroup': user_exitgroup,
		'eg': user_exitgroup,
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
		'dg': user_delgroup
	}
	switch[cmd](conn,args)

def close_clear_all(conn,fd):
	print("执行清理工作。。。")
	epoll.unregister(fd)
	#从在线userid2sock删除
	del_online_sock(conn)
	#从queue队列中删除
	del message_queue[fd]
	#从在线fd2sock字典删除
	if fd in auth_fd_to_socket.keys():
		del auth_fd_to_socket[fd] 
	fd_to_socket[fd].close()
	del fd_to_socket[fd]
	
	#从fd_to_sock列表删除
	#从epoll删除
	#epoll.unregister(fd)
	#关闭当前conn.close()
	#conn.close()

				
def start_server():
	#创建socket
	with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as s: 
		#绑定
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		s.bind((host,port))
		#监听
		s.listen(100)
		#设置非阻塞
		s.setblocking(False)
		#注册socket fileno到epoll上面
		epoll.register(s.fileno(),select.EPOLLIN)
		#文件描述符到socket字典
		fd_to_socket[s.fileno()]=s
		#文件描述符到队列的映射
		print("启动chat服务器%s:%s,并且注册到epoll!"%(host,port))
		print("等待客户端的连接...")
		while True:
			print("数量:%s ###userid_to_socket###:" % len(userid_to_socket),userid_to_socket)
			print("数量:%s ###auth_fd_to_socket###:" % len(auth_fd_to_socket),auth_fd_to_socket)
			print("数量:%s ###fd_to_socket###:" % len(fd_to_socket),fd_to_socket)
			print("数量:%s ###message_queue###:" % len(message_queue),message_queue)
			#轮询注册的事件集合，返回文件句柄，对应的事件
			events=epoll.poll(10)
			print("poll轮询超时...")
			if not events:
				print("epoll超时无活动连接，重新轮询......")
				continue
			print("有"+str(len(events))+"个新事件，开始处理......")
			#如果有事件发生,迭代读取事件,并且处理事件
			for fd,event in events: 
				#判断是否为服务器监听的socket
				print("fd的类型@@@@@@@@@@@@:",type(fd))
				print("当前事件ID:",'{0:08b}'.format(event))
				print("当前select.EPOLLIN事件:",'{0:08b}'.format(select.EPOLLIN))
				print("当前select.EPOLLOUT事件:",'{0:08b}'.format(select.EPOLLOUT))
				print("当前select.EPOLLHUP事件:",'{0:08b}'.format(select.EPOLLHUP))
				if fd == s.fileno():
					#处理新连接
					new_conn,addr=s.accept()
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
					#判断用户是否登录,如果没登录就转到EPOLLOUT
					c_sock=fd_to_socket[fd]
					if fd not in auth_fd_to_socket.keys():
						#获取数据
						try:
							length=struct.unpack('H',c_sock.recv(2))[0]
							data=c_sock.recv(int(length))
						except:
							print(u'[ %s ] 获取客户端数据异常' % time.strftime("%Y-%m-%d %X"))
							err_msg=u'sys 数据包异常'
							message_queue[fd].put(err_msg)
							#获取客户端数据异常，猜测客户端非正常关闭
							epoll.modify(fd,0)
							fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
							continue
						#判断包头字段里填入的长度和实际收到的是否一致	
						if len(data)!=length:
							print(u'[ %s ] 客户端数据包长度异常' % time.strftime("%Y-%m-%d %X"))
							err_msg=u'sys 数据包长度异常'
							message_queue[fd].put(err_msg)
							continue

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
										close_clear_all(c_sock,fd)
									continue

								#获取用户userid
								userid=user_name_userid(user)

								if userid < 0:
									login_err_msg=u'sys 用户名不存在！'
									message_queue[fd].put(login_err_msg)
									try:
										epoll.modify(fd,select.EPOLLOUT)	
									except:
										close_clear_all(c_sock,fd)
									continue
							
								#进行认证
								if user_auth(user,pwd):
									#注销其他客户端的连接
									single_login(user)
									#推送消息
									push_new_msg(c_sock,user)
									#添加到在线用户列表
									userid_to_socket[userid]=c_sock
									#添加到在线fd2sock列表
									auth_fd_to_socket[fd]=c_sock
									#提示登录成功的信息
									login_ok=u'sys 登录成功!'
									message_queue[fd].put(login_ok)
									try:
										print("[ %s ]开始修改epollout!" % time.ctime())
										epoll.modify(fd,select.EPOLLOUT)	
									except:
										close_clear_all(c_sock,fd)
									continue
								else:
									login_fail=u'sys 登录失败，密码错误！'
									message_queue[fd].put(login_fail)
									try:
										epoll.modify(fd,select.EPOLLOUT)	
									except:
										close_clear_all(c_sock,fd)
									continue
							#跳转到EPOLLOUT
							else:
								try:
									epoll.modify(fd,select.EPOLLOUT)	
								except:
									close_clear_all(c_sock,fd)
								continue

					#如果已登录，则提取数据，放入到Quene中，跳转到EPOLLOUT
					else:
						#获取数据
						try:
							length=struct.unpack('H',c_sock.recv(2))[0]
							data=c_sock.recv(int(length))
						except:
							print(u'[ %s ] 获取客户端数据异常' % time.strftime("%Y-%m-%d %X"))
							err_msg=u'sys 数据包异常'
							message_queue[fd].put(err_msg)
							epoll.modify(fd,0)
							fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
							continue
						#判断包头字段里填入的长度和实际收到的是否一致	
						if len(data)!=length:
							print(u'[ %s ] 客户端数据包长度异常' % time.strftime("%Y-%m-%d %X"))
							err_msg=u'sys 数据包异常'
							message_queue[fd].put(err_msg)
							try:
								epoll.modify(fd,select.EPOLLHUP)
							except:
								close_clear_all(c_sock,fd)
							continue
						
						#把数据存入队列
						message_queue[fd].put(data.decode('utf-8'))
						try:
							epoll.modify(fd,select.EPOLLOUT)	
						except:
							close_clear_all(c_sock,fd)
						
				#写事件
				elif event & select.EPOLLOUT:	
					print("[ %s ]进入epollout!" % time.ctime())
					#判断用户是否登录,如果没登录就推送登录提醒消息
					c_sock=fd_to_socket[fd]
					print(fd,auth_fd_to_socket.keys())
					if fd not in auth_fd_to_socket.keys():
						#获取队列中的内容
						try:
							message=message_queue[fd].get_nowait()
							send_msg(c_sock,message)
						except queue.Empty:
							prompt_auth_msg=u'请输入用户名密码进行登录!\n命令格式:auth username password'
							send_msg(c_sock,prompt_auth_msg)
						try:
							epoll.modify(fd,select.EPOLLIN)
						except:
							close_clear_all(c_sock,fd)
						continue
					#如果已经登录则推送，获取队列中的内容，判断消息的类别，调用不同的函数处理
					else:
						#获取队列中的内容
						try:
							message=message_queue[fd].get_nowait()
						except queue.Empty:
							try:
								epoll.modify(fd,select.EPOLLIN)
							except:
								close_clear_all(c_sock,fd)
							continue
						cmd=message.split(' ')[0]
						#判断是否是服务器的信息
						if cmd == 'sys':
							sys_reply=message.split(' ')[1:]
							send_msg(c_sock,' '.join(sys_reply))
						elif cmd == 'close':
							print(u'[ %s ] 客户端和服务器断开连接' % time.strftime("%Y-%m-%d %X"))
							send_msg(c_sock,u'您已经与服务器断开连接')
							#关闭连接
							epoll.modify(fd,0)
							auth_fd_to_socket[fd].shutdown(socket.SHUT_RDWR)
							continue
						elif cmd == 'auth':
							send_msg(c_sock,u'提示!您已认证！')
						elif cmd not in noargs_cmd+args_cmd:
							print(u'[ %s ] 客户端的命令不支持' % time.strftime("%Y-%m-%d %X"))
							send_msg(c_sock,u'输入的命令服务器不支持')
						else:
							args=message.split(' ')[1:]
							switch={
								'msg': user_msg,
								'm': user_msg,
								'gmsg': user_gmsg,
								'gm': user_gmsg,
								'adduser': user_adduser,
								'au': user_adduser,
								'deluser': user_deluser,
								'du': user_deluser,
								'addgroup': user_addgroup,
								'ag': user_addgroup,
								'exitgroup': user_exitgroup,
								'eg': user_exitgroup,
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
								'dg': user_delgroup
							}
							switch[cmd](c_sock,args)
						try:
							epoll.modify(fd,select.EPOLLIN)
						except:
							close_clear_all(c_sock,fd)
				#关闭事件
				elif event & select.EPOLLHUP:
					print("进入到EPOLLHUP中")
					close_clear_all(c_sock,fd)

if __name__=='__main__':
	start_server()
