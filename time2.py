# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 18:37:48 2017

@author: Rupert Wieser
"""
from time import *
import calendar


def __xxyyzz(t_struct, xxyyzz, start=0, separation=''):
    x = str(t_struct[start])[2:4]
    if len(x) == 0:
        x = str(t_struct[start])[0:2]
        if len(x) == 1:
            xxyyzz += str(0)
    xxyyzz += x + separation
    x = str(t_struct[start + 1])[0:2]
    if len(x) < 2:
        xxyyzz += str(0)
    xxyyzz += x + separation
    x = str(t_struct[start + 2])[0:2]
    if len(x) < 2:
        xxyyzz += str(0)
    xxyyzz += x
    return (xxyyzz)


def jjmmdd(time_step, separation=''):
    t_struct = gmtime(time_step)
    return (__xxyyzz(t_struct, '', 0, separation))


def jjjjmmdd(time_step, separation=''):
    t_struct = gmtime(time_step)
    return (__xxyyzz(t_struct, str(t_struct[0])[0:2], 0, separation))


def hhmmss(time_step, separation=''):
    t_struct = gmtime(time_step)
    return (__xxyyzz(t_struct, '', 3, separation))


def __timeListXXYYZZ(time_string, time_list, index = [0, 1, 2]):
    time_string = time_string.split(':')
    if len(time_string) == 1:
        time_string = time_string[0].split('/')
    if len(time_string) == 1:
        time_string = time_string[0].split('.')
    if len(time_string) == 1:
        time_string = time_string[0].split('-')
    j = 0
    if len(time_string) == 3:
        for i in index:
            time_list[i] = int(time_string[j])
            j += 1
    if  len(time_string) == 1:
        for i in index:
            time_list[i] = int(time_string[0][j*2:j*2+2])
            j += 1
    return(time_list)


def timeListJJMMDD(time_string, time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    t_list = __timeListXXYYZZ(time_string, time_list, [0, 1, 2])
    t_list[0] += 2000
    return (t_list)

def timeListDDMMJJ(time_string, time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    t_list = __timeListXXYYZZ(time_string, time_list, [2, 1, 0])
    t_list[0] += 2000
    return (t_list)

def timeListMMDDJJ(time_string, time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    t_list = __timeListXXYYZZ(time_string, time_list, [1, 2, 0])
    t_list[0] += 2000
    return (t_list)

def timeListHHMMSS(time_string, time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    return (__timeListXXYYZZ(time_string, time_list, [3, 4, 5]))

def timeListJJJJMMDD(time_string, time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    return (timeListJJMMDD(time_string[2:], time_list))

def timeListDDMMJJJJ(time_string, time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    return (timeListDDMMJJ(time_string[:-4]+time_string[-2:], time_list))

def timeListMMDDJJJJ(time_string, time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    return (timeListMMDDJJ(time_string[:-4]+time_string[-2:], time_list))

def timeListJJMMDDHHMMSS(JJMMDD_string, HHMMSS_string, time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    return (timeListHHMMSS(HHMMSS_string, timeListJJMMDD(JJMMDD_string, time_list)))

def timeListJJJJMMDDHHMMSS(JJJJMMDD_string, HHMMSS_string, time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    return(timeListHHMMSS(HHMMSS_string, timeListJJJJMMDD(JJJJMMDD_string, time_list)))

def timeListDDMMJJHHMMSS(DDMMJJ_string, HHMMSS_string, time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    return (timeListHHMMSS(HHMMSS_string, timeListDDMMJJ(DDMMJJ_string, time_list)))

def timeListDDMMJJJJHHMMSS(DDMMJJJJ_string, HHMMSS_string, time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    return(timeListHHMMSS(HHMMSS_string, timeListDDMMJJJJ(DDMMJJJJ_string, time_list)))

def timeListToTimestamp(TimeList):
    return(calendar.timegm(tuple(TimeList)))

def timeToTimestampJJJJMMDD_HHMMSS(JJJJMMDD_HHMMSSstring, delimiter=' ', time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    newStr = JJJJMMDD_HHMMSSstring.split(delimiter)
    if len(newStr[0]) > 14 or len(newStr[0]) < 8:
        return -1
    return(timeListToTimestamp(timeListHHMMSS(newStr[1], timeListJJJJMMDD(newStr[0], time_list))))

def timeToTimestampJJMMDD_HHMMSS(JJMMDD_HHMMSSstring, delimiter=' ', time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    newStr = JJMMDD_HHMMSSstring.split(delimiter)
    if len(newStr[0]) > 14 or len(newStr[0]) < 8:
        return -1
    return(timeListToTimestamp(timeListHHMMSS(newStr[1], timeListJJMMDD(newStr[0], time_list))))

def timeToTimestampDDMMJJJJ_HHMMSS(DDMMJJJJ_HHMMSSstring, delimiter=' ', time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    newStr = DDMMJJJJ_HHMMSSstring.split(delimiter)
    if len(newStr[0]) > 14 or len(newStr[0]) < 8:
        return -1
    return(timeListToTimestamp(timeListHHMMSS(newStr[1], timeListDDMMJJJJ(newStr[0], time_list))))

def timeToTimestampDDMMJJ_HHMMSS(DDMMJJ_HHMMSSstring, delimiter=' ', time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    newStr = DDMMJJ_HHMMSSstring.split(delimiter)
    if len(newStr[0]) > 14 or len(newStr[0]) < 8:
        return -1
    return(timeListToTimestamp(timeListHHMMSS(newStr[1], timeListDDMMJJ(newStr[0], time_list))))

def timeToTimestampMMDDJJJJ_HHMMSS(MMDDJJJJ_HHMMSSstring, delimiter=' ', time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    newStr = MMDDJJJJ_HHMMSSstring.split(delimiter)
    if len(newStr[0]) > 14 or len(newStr[0]) < 8:
        return -1
    return(timeListToTimestamp(timeListHHMMSS(newStr[1], timeListMMDDJJJJ(newStr[0], time_list))))

def timeToTimestampMMDDJJ_HHMMSS(MMDDJJJJ_HHMMSSstring, delimiter=' ', time_list=[0, 0, 0, 0, 0, 0, 0, 0, 0]):
    newStr = MMDDJJJJ_HHMMSSstring.split(delimiter)
    if len(newStr[0]) > 14 or len(newStr[0]) < 8:
        return -1
    return(timeListToTimestamp(timeListHHMMSS(newStr[1], timeListMMDDJJ(newStr[0], time_list))))

def timestampToJJJJMMDD_HHMMSS(timestamp=0,deliminiter1="",deliminiter2="",deliminiter3=""):
    times = list(gmtime(timestamp))
    rettime = ""
    for i in range(len(times)):
        val = str(times[i])
        if i < 2:
            deli = deliminiter1
        elif i == 2:
            deli = deliminiter2
        elif i < 5:
            deli = deliminiter3
        elif i == 5:
            deli = ''
        else:
            break
        if len(str(times[i])) < 2:
            val = "0" + val
        rettime += val + deli
    return(rettime)

def testall():
    print(timeToTimestampDDMMJJ_HHMMSS("12-08-19 22:11:33"))
    print(timeToTimestampDDMMJJJJ_HHMMSS("12/08/2019 22.11.33"))
    print(timeToTimestampMMDDJJ_HHMMSS("08.12.19 22-11-33"))
    print(timeToTimestampMMDDJJJJ_HHMMSS("08:12:2019 22/11/33"))
    print(timeToTimestampJJMMDD_HHMMSS("19.08.12 22-11-33"))
    print(timeToTimestampJJJJMMDD_HHMMSS("2019-08-12 22:11:33"))