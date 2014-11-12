#coding=utf-8
__author__ = 'qianfunian'

import MySQLdb
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib


try:
    mobileDbConnection = MySQLdb.connect(user="readonly_v2", passwd="aNjuKe9dx1Pdw", host='10.10.8.128', db="mobile_db")
    mobileCursor = mobileDbConnection.cursor()

    AJKDBConnection = MySQLdb.connect(user="readonly_v2", passwd="aNjuKe9dx1Pdw", host='10.10.8.80', db="anjuke_db")
    AJKCursor = AJKDBConnection.cursor()

    globalBrokerIds = {}

    lastId = 0

    def generateData(brokerId, day):
        sql = "select count(distinct community_id) as count from broker_community_sign_201411 where broker_id = " + str(
            brokerId) + " and sign_time like '2014-11-" + str(day) + "%' "
        mobileCursor.execute(sql)
        data = mobileCursor.fetchall()

        sql = 'select ajk_brokerextend.BrokerId,ajk_brokerextend.TrueName,ajk_brokerextend.UserMobile,' \
              'ajk_brokerextend.CityId,ajk_brokerextend.BelongCom,ajk_citytype.CityName from ajk_brokerextend ' \
              'left join ajk_citytype on ajk_brokerextend.CityId=ajk_citytype.CityId where BrokerId= ' + str(
            brokerId)

        AJKCursor.execute(sql)
        brokerInfo = AJKCursor.fetchall()
        file_object = open('2014-11-' + str(day) + '.csv', 'a+')
        for info in brokerInfo[0]:
            file_object.write(str(info) + ',')

        file_object.write(str(data[0][0]) + '\n')
        file_object.close()


    flag = open('flag.txt', 'r')
    start = flag.read()
    flag.close()
    end = int(start) + 20

    today = time.strftime("%d", time.localtime())
    interrupt = 0
    while True:
        if (interrupt == 1):
            break
        sql = "select id,broker_id,DATE_FORMAT(sign_time,'%d') as sign_time from broker_community_sign_201411 where id > " + str(
            start) + " and id<= " + str(end) + " order by id asc"
        mobileCursor.execute(sql)
        signInfo = mobileCursor.fetchall()
        if (signInfo == ()):
            write_file = open('flag.txt', 'w')
            write_file.write(str(lastId))
            write_file.close()
            break

        for info in signInfo:
            if (globalBrokerIds.has_key(str(info[2]) + str(info[1]))):
                continue
            if (str(info[2]) == today):
                write_file = open('flag.txt', 'w')
                write_file.write(str(info[0] - 1))
                write_file.close()
                interrupt = 1
                break
            globalBrokerIds[str(info[2]) + str(info[1])] = ''
            lastId = info[0]
            generateData(info[1], info[2])
        start = end
        end += 10

except Exception, e:
    print(str(e))

exit()