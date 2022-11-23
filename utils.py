import re
import os
import sys
# from Logger import LogEventSourcing
from datetime import datetime
import dateutil.parser
import traceback
import time
import uuid


# logger = LogEventSourcing()

def send_email(user, pwd, recipient, subject, body):
	import smtplib

	gmail_user = user
	gmail_pwd = pwd
	FROM = user
	TO = recipient if type(recipient) is list else [recipient]
	SUBJECT = subject
	TEXT = body

	# Prepare actual message
	message = """From: %s\nTo: %s\nSubject: %s\n\n%s
    """ % (FROM, ", ".join(TO), SUBJECT, TEXT)
	try:
		server = smtplib.SMTP("smtp.gmail.com", 587)
		server.ehlo()
		server.starttls()
		server.login(gmail_user, gmail_pwd)
		server.sendmail(FROM, TO, message)
		server.close()
		return 'successfully sent the mail'
	except:
		return "failed to send mail" + str(traceback.format_exc())


def uprint(*objects, sep=' ', end='\n', file=sys.stdout):
	enc = file.encoding
	if enc == 'UTF-8':
		print(*objects, sep=sep, end=end, file=file)
	else:
		f = lambda obj: str(obj).encode(enc, errors='backslashreplace').decode(enc)
		print(*map(f, objects), sep=sep, end=end, file=file)


def wirtefile(file, txt):
	# print('write file')
	with open(file, "a", encoding='utf-8') as myfile:
		myfile.write(txt + '\n')


def getguid():
	return str(uuid.uuid4())


def checktype(val):
	if type(val) is list:
		print('a list')
	elif type(val) is tuple:
		print('a tuple')
	elif type(val) is dict:
		print('a dict')
	elif type(val) is str:
		print('a string')
	# logger.info(val)
	# wirtefile('C:/LogFile/action1111.log', val)
	elif type(val) is int:
		print('a int')
	elif type(val) is float:
		print('a float')
	else:
		print('neither a type')
		uprint(val)


def intTryParse(value, default=0):
	try:
		return int(value)
	except:
		return default


def convertDateStamp(val):
	try:
		if type(val) is str:
			d = dateutil.parser.parse(val)
			return time.mktime(d.timetuple())
		else:
			return time.mktime(val.timetuple())
	except:
		return 0

def convert_date_to_timestamp(val):
	try:
		if type(val) is str:
			d = dateutil.parser.parse(val)
			return time.mktime(d.timetuple())
		else:
			return time.mktime(val.timetuple())
	except:
		return 0


def datetimeTryParse(val, format='', default=None):
	try:
		if val is None:
			return default
		if format == '':
			return dateutil.parser.parse(val)
		else:
			return datetime.strptime(val, format)
	except:
		print(traceback.format_exc())
		return default


def dateTryParse(val, format='', default=None):
	try:
		return datetime.strptime(val, format).strftime("%Y-%m-%d")
	except:
		return default


uniChars = "àáảãạâầấẩẫậăằắẳẵặèéẻẽẹêềếểễệđìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵÀÁẢÃẠÂẦẤẨẪẬĂẰẮẲẴẶÈÉẺẼẸÊỀẾỂỄỆĐÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴÂĂĐÔƠƯ"
unsignChars = "aaaaaaaaaaaaaaaaaeeeeeeeeeeediiiiiooooooooooooooooouuuuuuuuuuuyyyyyAAAAAAAAAAAAAAAAAEEEEEEEEEEEDIIIIIOOOOOOOOOOOOOOOOOUUUUUUUUUUUYYYYYAADOOU"


def loaddicchar():
	dic = {}
	char1252 = 'à|á|ả|ã|ạ|ầ|ấ|ẩ|ẫ|ậ|ằ|ắ|ẳ|ẵ|ặ|è|é|ẻ|ẽ|ẹ|ề|ế|ể|ễ|ệ|ì|í|ỉ|ĩ|ị|ò|ó|ỏ|õ|ọ|ồ|ố|ổ|ỗ|ộ|ờ|ớ|ở|ỡ|ợ|ù|ú|ủ|ũ|ụ|ừ|ứ|ử|ữ|ự|ỳ|ý|ỷ|ỹ|ỵ|À|Á|Ả|Ã|Ạ|Ầ|Ấ|Ẩ|Ẫ|Ậ|Ằ|Ắ|Ẳ|Ẵ|Ặ|È|É|Ẻ|Ẽ|Ẹ|Ề|Ế|Ể|Ễ|Ệ|Ì|Í|Ỉ|Ĩ|Ị|Ò|Ó|Ỏ|Õ|Ọ|Ồ|Ố|Ổ|Ỗ|Ộ|Ờ|Ớ|Ở|Ỡ|Ợ|Ù|Ú|Ủ|Ũ|Ụ|Ừ|Ứ|Ử|Ữ|Ự|Ỳ|Ý|Ỷ|Ỹ|Ỵ'.split(
		'|')
	charutf8 = "à|á|ả|ã|ạ|ầ|ấ|ẩ|ẫ|ậ|ằ|ắ|ẳ|ẵ|ặ|è|é|ẻ|ẽ|ẹ|ề|ế|ể|ễ|ệ|ì|í|ỉ|ĩ|ị|ò|ó|ỏ|õ|ọ|ồ|ố|ổ|ỗ|ộ|ờ|ớ|ở|ỡ|ợ|ù|ú|ủ|ũ|ụ|ừ|ứ|ử|ữ|ự|ỳ|ý|ỷ|ỹ|ỵ|À|Á|Ả|Ã|Ạ|Ầ|Ấ|Ẩ|Ẫ|Ậ|Ằ|Ắ|Ẳ|Ẵ|Ặ|È|É|Ẻ|Ẽ|Ẹ|Ề|Ế|Ể|Ễ|Ệ|Ì|Í|Ỉ|Ĩ|Ị|Ò|Ó|Ỏ|Õ|Ọ|Ồ|Ố|Ổ|Ỗ|Ộ|Ờ|Ớ|Ở|Ỡ|Ợ|Ù|Ú|Ủ|Ũ|Ụ|Ừ|Ứ|Ử|Ữ|Ự|Ỳ|Ý|Ỷ|Ỹ|Ỵ".split(
		'|')
	for i in range(len(char1252)):
		dic[char1252[i]] = charutf8[i]
	return dic


dicchar = loaddicchar()


def convertwindown1525toutf8(txt):
	return re.sub(
		r'à|á|ả|ã|ạ|ầ|ấ|ẩ|ẫ|ậ|ằ|ắ|ẳ|ẵ|ặ|è|é|ẻ|ẽ|ẹ|ề|ế|ể|ễ|ệ|ì|í|ỉ|ĩ|ị|ò|ó|ỏ|õ|ọ|ồ|ố|ổ|ỗ|ộ|ờ|ớ|ở|ỡ|ợ|ù|ú|ủ|ũ|ụ|ừ|ứ|ử|ữ|ự|ỳ|ý|ỷ|ỹ|ỵ|À|Á|Ả|Ã|Ạ|Ầ|Ấ|Ẩ|Ẫ|Ậ|Ằ|Ắ|Ẳ|Ẵ|Ặ|È|É|Ẻ|Ẽ|Ẹ|Ề|Ế|Ể|Ễ|Ệ|Ì|Í|Ỉ|Ĩ|Ị|Ò|Ó|Ỏ|Õ|Ọ|Ồ|Ố|Ổ|Ỗ|Ộ|Ờ|Ớ|Ở|Ỡ|Ợ|Ù|Ú|Ủ|Ũ|Ụ|Ừ|Ứ|Ử|Ữ|Ự|Ỳ|Ý|Ỷ|Ỹ|Ỵ',
		lambda x: dicchar[x.group()], txt)


def convertnumtostringsort(input, numchar):
	txt = str(input)
	while len(txt) < numchar:
		txt = '0' + txt
	return txt


def unicode_to_unsigned(s):
	if s is None or type(s) is not str:
		return None
	res = ''
	for idx, item in enumerate(s):
		pos = uniChars.find(item)
		if pos >= 0:
			res += unsignChars[pos]
		else:
			res += s[idx]
	return res

def substring_to_dot(txt, max_len):
	if not txt or len(txt) < max_len:
		return txt
	output = txt[:max_len - 3]
	idx = output.rfind(' ')
	if idx > 0:
		output = output[:idx] + ' ...'
		return output
	else:
		output = output + '...'
	return output


def build_video_url(_videoid, _name):
	return "/{1}-{0}.htm".format(_videoid, unicode_to_kodauvagach(_name).replace("\"", "").replace("'", "")).lower()


def unicode_to_kodauvagach(txt):
	_char = "abcdefghijklmnopqrstxyzuvxw0123456789/- "
	s = unicode_to_unsigned(txt.lower().strip())
	res = ''
	for idx, item in enumerate(s):
		if _char.find(item) >= 0:
			if item != ' ':
				res += item
			elif idx > 0 and s[idx - 1] != ' ' and s[idx - 1] != '-':
				res += "-"
	res = res.replace("/", "-")
	return res.replace("--", "-").strip('-').lower()


def checklog(keyword, infile, outfile, starttime, endtime=None, getdataredis=True):
	arrsesion = []
	wordsplit = ' - '

	if endtime == None or endtime == '':
		with open(infile) as f:
			lines = f.readlines()
			flag = False
			for line in lines:
				if line.startswith(starttime):
					flag = True
				if flag:
					if line.find(keyword) >= 0:
						data = line[line.rfind(wordsplit) + len(wordsplit):]

						arr = data.split('\t')
						arrsesion.append(arr[0])
	else:
		with open(infile) as f:
			lines = f.readlines()
			flag = False
			for line in lines:
				if line.startswith(starttime):
					flag = True
				if flag and line.startswith(endtime):
					break
				if flag:
					if line.find(keyword) >= 0:
						data = line[line.rfind(wordsplit) + len(wordsplit):]
						arr = data.split('\t')
						arrsesion.append(arr[0])
	# print(arrsesion,len(arrsesion))
	return arrsesion
	if not getdataredis:
		wirtefile(outfile, str(arrsesion))
		return arrsesion
	from _dbconnection.redisclient import RedisClient
	import config
	dbredis = config.confRedisNotify
	keys = list(map(lambda x: 'SohaNews:queuecd:' + x, arrsesion))
	res = RedisClient(dbredis).GetValues(keys)
	# print(res)
	wirtefile(outfile, str(res))
	return res

