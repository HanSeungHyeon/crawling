import requests
import pymysql
import datetime
import re
import time

from urllib.request import urlopen
from bs4 import BeautifulSoup

conn = pymysql.connect(host='localhost', user='root', password='rmfhqltm!7782', db='crawling', charset='utf8')
cur = conn.cursor()

HEADERS = {'User-Agent': 'Mozilla/5.0', 'Accept-Encoding': 'gzip, deflate', 'Accept': '*/*', 'Connection': 'keep-alive'}

URL = 'https://www.schoolinfo.go.kr/ei/ss/pneiss_a08_s0.do' #크롤링 할 홈페이지의 URL
page_source = requests.get(url=URL, headers=HEADERS, verify=False).text;         #URL의 페이지 소스를 텍스트로 가져온다.
#page_source = requests.get(url=URL, verify=False).text;         #URL의 페이지 소스를 텍스트로 가져온다.

date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'); #DB에 넣을 시간 값을 설정.

sido_name = "";             #시도의 이름을 저장할 변수
sido_code = "";             #시도의 코드값을 저장할 변수
school_gubun = "";          #설립구분
school_gubun2 = "";         #설립유형
school_seolip_date = "";    #설립일자
school_student_cnt = "";    #학생수
school_teacher_cnt = "";    #교원수
school_place = "";          #체육집회공간
school_number = "";         #대표번호
school_fax = "";            #팩스
school_office_number = "";  #행정실
school_tearcher_room = "";  #교무실
school_homepage = "";       #홈페이지
school_address = "";        #주소
school_control = "";        #관할교육청
school_character = "";      #학교유형 (고등학교에만 존재)

sidoes = BeautifulSoup(page_source, "html.parser")
for sido in sidoes.find_all('option'):  #bs로 가져온 드라이버의 페이지 소스값에서 option인 값을 가져온다.(시도 코드, 시도 이름)
  sido_nm = sido.text
  sido_code = sido.get('value')
  if sido_code == '':
    continue;
  cur.execute(f"REPLACE INTO tb_sido (SIDO_CODE,SIDO_NAME,SIDO_UPDATE_DATE) VALUES(\"{sido_code}\",\"{sido_nm}\",\"{date}\")")
  conn.commit()

sigungus = re.findall('var adrcdIdLastNm = (.+?);',page_source) #시군구의 이름
sigungu_index = 1

for sigungu_info in sigungus:
  sigungu_name = sigungu_info[1:len(sigungu_info)-1];
  cur.execute(f"INSERT IGNORE INTO tb_sigungu (SIGUNGU_NAME, SIGUNGU_UPDATE_DATE, SIGUNGU_INDEX) VALUES(\"{sigungu_name}\",\"{date}\",\"{sigungu_index}\")")
  conn.commit()
  sigungu_index+=1

matched = re.findall('var adrcdId = (.+?);',page_source) #시군구의 code    시도code는 시군구code의 앞 2글자 + 00000000이다.
sigungu_index = 1
for match in matched:
  length = len(match)-1;
  sigungu_code = match[1:length];  
  if sido_code != match[1:3]+'00000000':
    sido_code = match[1:3]+'00000000'

  cur.execute(f"UPDATE tb_sigungu SET SIDO_CODE = \"{sido_code}\", SIGUNGU_CODE = \"{sigungu_code}\", SIGUNGU_UPDATE_DATE = \"{date}\"  WHERE SIGUNGU_INDEX = \"{sigungu_index}\"")
  conn.commit()
  ##학교정보 여기서 불러오기 시작함.
  school_param = param = {'SIDO_CODE':sido_code, 'GUGUN_CODE':sigungu_code}
  school_request = requests.post(url = URL, data=param, headers=HEADERS, verify=False)

  school_bs = BeautifulSoup(school_request.text, "html.parser")
  schools = school_bs.find_all('ul', attrs={"class":"link_list"})
  school = schools[0].find_all("a")  #0:초등학교
  link = "";
  for school_detail in school:
    school_name = school_detail.get_text()
    school_link = school_detail.get('href')
    link = school_link[24:len(school_link)-2]
    cur.execute(f"REPLACE INTO tb_school (SCHOOL_NAME, SCHOOL_CODE, SCHOOL_GUBUN, SCHOOL_SIDO_CODE, SCHOOL_SIGUNGU_CODE, SCHOOL_UPDATE_DATE) VALUES (\"{school_name}\",\"{link}\",'0',\"{sido_code}\",\"{sigungu_code}\",\"{date}\")")
    conn.commit();
    school_info_param = {'HG_CD':link, 'PRE_JG':'', 'GS_HANGMOK_CD':''}
    try:
      school_info_request = requests.post(url = 'https://www.schoolinfo.go.kr/ei/ss/Pneiss_b01_s0.do',params=school_info_param, headers=HEADERS, verify=False)
    except:
      time.sleep(5)
      school_info_request = requests.post(url = 'https://www.schoolinfo.go.kr/ei/ss/Pneiss_b01_s0.do',params=school_info_param, headers=HEADERS, verify=False)
    
    school_info_bs = BeautifulSoup(school_info_request.text, "html.parser")
    schools_detail_info = school_info_bs.find_all('span', attrs={"class":"md"})
    cnt = 0
    for school_detail_info in schools_detail_info:
      school_detail_info = school_detail_info.get_text().split(":")
      school_detail_info[0]=school_detail_info[0].strip().replace("\n", "")
      school_detail_info[1]=school_detail_info[1].strip().replace("\n", "")
      school_detail_info[1]=school_detail_info[1].strip().replace("\t", "")
      if cnt == 0:
        school_gubun = school_detail_info[1];
      if cnt == 1:
        school_gubun2 = school_detail_info[1];
      if cnt == 2:
        school_seolip_date = school_detail_info[1];
      if cnt == 3:
        school_student_cnt = school_detail_info[1].split('(',1)[0].strip();
      if cnt == 4:
        school_teacher_cnt = school_detail_info[1].split('(',1)[0].strip();
      if cnt == 5:
        school_place = school_detail_info[1];
      if cnt == 6:
        school_number = school_detail_info[1]
      if cnt == 7:
        school_fax = school_detail_info[1];
      if cnt == 8:
        school_office_number = school_detail_info[1];
      if cnt == 9:
        school_tearcher_room = school_detail_info[1];
      if cnt == 10:
        homepage = school_info_bs.find_all('a', attrs={"class":"alink"})
        school_detail_info[1] = homepage[0].get('href')
        school_homepage = school_detail_info[1];
      if cnt == 11:
        school_address = school_detail_info[1];
      if cnt == 12:
        school_control = school_detail_info[1];
      cnt += 1
    cur.execute(f"REPLACE INTO tb_school_info (SCHOOL_CODE,SCHOOL_INFO_SEOLLIP_GUBUN, SCHOOL_INFO_SEOLLIP, SCHOOL_INFO_SEOLLIP_DATE, SCHOOL_INFO_STUDENT_COUNT, SCHOOL_INFO_STAFF_COUNT, SCHOOL_INFO_EXERCISE_PLACE, SCHOOL_INFO_OWNER_NUMBER, SCHOOL_INFO_FAX_NUMBER, SCHOOL_INFO_OFFICE_NUMBER, SCHOOL_INFO_TEACHER_NUMBER, SCHOOL_INFO_HOMEPAGE, SCHOOL_INFO_ADDRESS, SCHOOL_INFO_CONTROL_CENTER, SCHOOL_INFO_UPDATE_DATE) values (\"{link}\",\"{school_gubun}\",\"{school_gubun2}\",\"{school_seolip_date}\",\"{school_student_cnt}\",\"{school_teacher_cnt}\",\"{school_place}\",\"{school_number}\",\"{school_fax}\",\"{school_office_number}\",\"{school_tearcher_room}\",\"{school_homepage}\",\"{school_address}\",\"{school_control}\",\"{date}\")")
    conn.commit()

  school = schools[1].find_all("a")  #1:중학교
  for school_detail in school:
    school_name = school_detail.get_text()
    school_link = school_detail.get('href')
    link = school_link[24:len(school_link)-2]
    cur.execute(f"REPLACE INTO tb_school (SCHOOL_NAME, SCHOOL_CODE, SCHOOL_GUBUN, SCHOOL_SIDO_CODE, SCHOOL_SIGUNGU_CODE, SCHOOL_UPDATE_DATE) VALUES (\"{school_name}\",\"{link}\",'1',\"{sido_code}\",\"{sigungu_code}\",\"{date}\")")
    conn.commit();
    school_info_param = {'HG_CD':link, 'PRE_JG':'', 'GS_HANGMOK_CD':''}
    try:
      school_info_request = requests.post(url = 'https://www.schoolinfo.go.kr/ei/ss/Pneiss_b01_s0.do',params=school_info_param, headers=HEADERS, verify=False)
    except:
      time.sleep(5)
      school_info_request = requests.post(url = 'https://www.schoolinfo.go.kr/ei/ss/Pneiss_b01_s0.do',params=school_info_param, headers=HEADERS, verify=False)
    
    school_info_bs = BeautifulSoup(school_info_request.text, "html.parser")
    schools_detail_info = school_info_bs.find_all('span', attrs={"class":"md"})
    cnt = 0
    for school_detail_info in schools_detail_info:
      school_detail_info = school_detail_info.get_text().split(":")
      school_detail_info[0]=school_detail_info[0].strip().replace("\n", "")
      school_detail_info[1]=school_detail_info[1].strip().replace("\n", "")
      school_detail_info[1]=school_detail_info[1].strip().replace("\t", "")
      if cnt == 0:
        school_gubun = school_detail_info[1];
      if cnt == 1:
        school_gubun2 = school_detail_info[1];
      if cnt == 2:
        school_seolip_date = school_detail_info[1];
      if cnt == 3:
        school_student_cnt = school_detail_info[1].split('(',1)[0].strip();
      if cnt == 4:
        school_teacher_cnt = school_detail_info[1].split('(',1)[0].strip();
      if cnt == 5:
        school_place = school_detail_info[1];
      if cnt == 6:
        school_number = school_detail_info[1];
      if cnt == 7:
        school_fax = school_detail_info[1];
      if cnt == 8:
        school_office_number = school_detail_info[1];
      if cnt == 9:
        school_tearcher_room = school_detail_info[1];
      if cnt == 10:
        homepage = school_info_bs.find_all('a', attrs={"class":"alink"})
        school_detail_info[1] = homepage[0].get('href')
        school_homepage = school_detail_info[1];
      if cnt == 11:
        school_address = school_detail_info[1];
      if cnt == 12:
        school_control = school_detail_info[1];
      cnt += 1
    cur.execute(f"REPLACE INTO tb_school_info (SCHOOL_CODE,SCHOOL_INFO_SEOLLIP_GUBUN, SCHOOL_INFO_SEOLLIP, SCHOOL_INFO_SEOLLIP_DATE, SCHOOL_INFO_STUDENT_COUNT, SCHOOL_INFO_STAFF_COUNT, SCHOOL_INFO_EXERCISE_PLACE, SCHOOL_INFO_OWNER_NUMBER, SCHOOL_INFO_FAX_NUMBER, SCHOOL_INFO_OFFICE_NUMBER, SCHOOL_INFO_TEACHER_NUMBER, SCHOOL_INFO_HOMEPAGE, SCHOOL_INFO_ADDRESS, SCHOOL_INFO_CONTROL_CENTER, SCHOOL_INFO_UPDATE_DATE) values (\"{link}\",\"{school_gubun}\",\"{school_gubun2}\",\"{school_seolip_date}\",\"{school_student_cnt}\",\"{school_teacher_cnt}\",\"{school_place}\",\"{school_number}\",\"{school_fax}\",\"{school_office_number}\",\"{school_tearcher_room}\",\"{school_homepage}\",\"{school_address}\",\"{school_control}\",\"{date}\")")
    conn.commit()

  school = schools[2].find_all("a")  #2:고등학교
  for school_detail in school:
    school_name = school_detail.get_text()
    school_link = school_detail.get('href')
    link = school_link[24:len(school_link)-2]
    cur.execute(f"REPLACE INTO tb_school (SCHOOL_NAME, SCHOOL_CODE, SCHOOL_GUBUN, SCHOOL_SIDO_CODE, SCHOOL_SIGUNGU_CODE, SCHOOL_UPDATE_DATE) VALUES (\"{school_name}\",\"{link}\",'2',\"{sido_code}\",\"{sigungu_code}\",\"{date}\")")
    conn.commit();
    school_info_param = {'HG_CD':link, 'PRE_JG':'', 'GS_HANGMOK_CD':''}
    try:
      school_info_request = requests.post(url = 'https://www.schoolinfo.go.kr/ei/ss/Pneiss_b01_s0.do',params=school_info_param, headers=HEADERS, verify=False)
    except:
      time.sleep(5)
      school_info_request = requests.post(url = 'https://www.schoolinfo.go.kr/ei/ss/Pneiss_b01_s0.do',params=school_info_param, headers=HEADERS, verify=False)

    school_info_bs = BeautifulSoup(school_info_request.text, "html.parser")
    schools_detail_info = school_info_bs.find_all('span', attrs={"class":"md"})
    cnt = 0
    for school_detail_info in schools_detail_info:
      school_detail_info = school_detail_info.get_text().split(":")
      school_detail_info[0]=school_detail_info[0].strip().replace("\n", "")
      school_detail_info[1]=school_detail_info[1].strip().replace("\n", "")
      school_detail_info[1]=school_detail_info[1].strip().replace("\t", "")

      if cnt == 0:
        school_gubun = school_detail_info[1];
      if cnt == 1:
        school_gubun2 = school_detail_info[1];
      if cnt == 2:
        school_character = school_detail_info[1];
      if cnt == 3:
        school_seolip_date = school_detail_info[1];
      if cnt == 4:
        school_student_cnt = school_detail_info[1].split('(',1)[0].strip();
      if cnt == 5:
        school_teacher_cnt = school_detail_info[1].split('(',1)[0].strip();
      if cnt == 6:
        school_place = school_detail_info[1];
      if cnt == 7:
        school_number = school_detail_info[1]
      if cnt == 8:
        school_fax = school_detail_info[1];
      if cnt == 9:
        school_office_number = school_detail_info[1];
      if cnt == 10:
        school_tearcher_room = school_detail_info[1];
      if cnt == 11:
        homepage = school_info_bs.find_all('a', attrs={"class":"alink"})
        school_detail_info[1] = homepage[0].get('href')
        school_homepage = school_detail_info[1];
      if cnt == 12:
        school_address = school_detail_info[1];
      if cnt == 13:
        school_control = school_detail_info[1];
      cnt += 1
    cur.execute(f"REPLACE INTO tb_school_info (SCHOOL_CODE,SCHOOL_INFO_SEOLLIP_GUBUN, SCHOOL_INFO_SEOLLIP, SCHOOL_INFO_SEOLLIP_DATE, SCHOOL_INFO_STUDENT_COUNT, SCHOOL_INFO_STAFF_COUNT, SCHOOL_INFO_EXERCISE_PLACE, SCHOOL_INFO_OWNER_NUMBER, SCHOOL_INFO_FAX_NUMBER, SCHOOL_INFO_OFFICE_NUMBER, SCHOOL_INFO_TEACHER_NUMBER, SCHOOL_INFO_HOMEPAGE, SCHOOL_INFO_ADDRESS, SCHOOL_INFO_CONTROL_CENTER, SCHOOL_INFO_UPDATE_DATE) values (\"{link}\",\"{school_gubun}\",\"{school_gubun2}\",\"{school_seolip_date}\",\"{school_student_cnt}\",\"{school_teacher_cnt}\",\"{school_place}\",\"{school_number}\",\"{school_fax}\",\"{school_office_number}\",\"{school_tearcher_room}\",\"{school_homepage}\",\"{school_address}\",\"{school_control}\",\"{date}\")")
    conn.commit()

  school = schools[3].find_all("a")  #3:기타
  for school_detail in school:
    school_name = school_detail.get_text()
    school_link = school_detail.get('href')
    link = school_link[24:len(school_link)-2]
    cur.execute(f"REPLACE INTO tb_school (SCHOOL_NAME, SCHOOL_CODE, SCHOOL_GUBUN, SCHOOL_SIDO_CODE, SCHOOL_SIGUNGU_CODE, SCHOOL_UPDATE_DATE) VALUES (\"{school_name}\",\"{link}\",'3',\"{sido_code}\",\"{sigungu_code}\",\"{date}\")")
    conn.commit();
    school_info_param = {'HG_CD':link, 'PRE_JG':'', 'GS_HANGMOK_CD':''}
    try:
      school_info_request = requests.post(url = 'https://www.schoolinfo.go.kr/ei/ss/Pneiss_b01_s0.do',params=school_info_param, headers=HEADERS, verify=False)
    except:
      time.sleep(5)
      school_info_request = requests.post(url = 'https://www.schoolinfo.go.kr/ei/ss/Pneiss_b01_s0.do',params=school_info_param, headers=HEADERS, verify=False)

    school_info_bs = BeautifulSoup(school_info_request.text, "html.parser")
    schools_detail_info = school_info_bs.find_all('span', attrs={"class":"md"})
    cnt = 0
    if len(schools_detail_info) == 14:
          for school_detail_info in schools_detail_info:
            school_detail_info = school_detail_info.get_text().split(":")
            school_detail_info[0]=school_detail_info[0].strip().replace("\n", "")
            school_detail_info[1]=school_detail_info[1].strip().replace("\n", "")
            school_detail_info[1]=school_detail_info[1].strip().replace("\t", "")
            if cnt == 0:
              school_gubun = school_detail_info[1];
            if cnt == 1:
              school_gubun2 = school_detail_info[1];
            if cnt == 2:
              school_character = school_detail_info[1];
            if cnt == 3:
              school_seolip_date = school_detail_info[1];
            if cnt == 4:
              school_student_cnt = school_detail_info[1].split('(',1)[0].strip();
            if cnt == 5:
              school_teacher_cnt = school_detail_info[1].split('(',1)[0].strip();
            if cnt == 6:
              school_place = school_detail_info[1];
            if cnt == 7:
              school_number = school_detail_info[1]
            if cnt == 8:
              school_fax = school_detail_info[1];
            if cnt == 9:
              school_office_number = school_detail_info[1];
            if cnt == 10:
              school_tearcher_room = school_detail_info[1];
            if cnt == 11:
              homepage = school_info_bs.find_all('a', attrs={"class":"alink"})
              school_detail_info[1] = homepage[0].get('href')
              school_homepage = school_detail_info[1];
            if cnt == 12:
              school_address = school_detail_info[1];
            if cnt == 13:
              school_control = school_detail_info[1];
            cnt += 1
          cur.execute(f"REPLACE INTO tb_school_info (SCHOOL_CODE,SCHOOL_INFO_SEOLLIP_GUBUN, SCHOOL_INFO_SEOLLIP, SCHOOL_INFO_SEOLLIP_DATE, SCHOOL_INFO_STUDENT_COUNT, SCHOOL_INFO_STAFF_COUNT, SCHOOL_INFO_EXERCISE_PLACE, SCHOOL_INFO_OWNER_NUMBER, SCHOOL_INFO_FAX_NUMBER, SCHOOL_INFO_OFFICE_NUMBER, SCHOOL_INFO_TEACHER_NUMBER, SCHOOL_INFO_HOMEPAGE, SCHOOL_INFO_ADDRESS, SCHOOL_INFO_CONTROL_CENTER, SCHOOL_INFO_UPDATE_DATE) values (\"{link}\",\"{school_gubun}\",\"{school_gubun2}\",\"{school_seolip_date}\",\"{school_student_cnt}\",\"{school_teacher_cnt}\",\"{school_place}\",\"{school_number}\",\"{school_fax}\",\"{school_office_number}\",\"{school_tearcher_room}\",\"{school_homepage}\",\"{school_address}\",\"{school_control}\",\"{date}\")")
          conn.commit()
    else:
      for school_detail_info in schools_detail_info:
        school_detail_info = school_detail_info.get_text().split(":")
        school_detail_info[0]=school_detail_info[0].strip().replace("\n", "")
        school_detail_info[1]=school_detail_info[1].strip().replace("\n", "")
        school_detail_info[1]=school_detail_info[1].strip().replace("\t", "")
        if cnt == 0:
          school_gubun = school_detail_info[1];
        if cnt == 1:
          school_gubun2 = school_detail_info[1];
        if cnt == 2:
          school_seolip_date = school_detail_info[1];
        if cnt == 3:
          school_student_cnt = school_detail_info[1].split('(',1)[0].strip();
        if cnt == 4:
          school_teacher_cnt = school_detail_info[1].split('(',1)[0].strip();
        if cnt == 5:
          school_place = school_detail_info[1];
        if cnt == 6:
          school_number = school_detail_info[1]
        if cnt == 7:
          school_fax = school_detail_info[1];
        if cnt == 8:
          school_office_number = school_detail_info[1];
        if cnt == 9:
          school_tearcher_room = school_detail_info[1];
        if cnt == 10:
          homepage = school_info_bs.find_all('a', attrs={"class":"alink"})
          school_detail_info[1] = homepage[0].get('href')
          school_homepage = school_detail_info[1];
        if cnt == 11:
          school_address = school_detail_info[1];
        if cnt == 12:
          school_control = school_detail_info[1];
        cnt += 1
      cur.execute(f"REPLACE INTO tb_school_info (SCHOOL_CODE,SCHOOL_INFO_SEOLLIP_GUBUN, SCHOOL_INFO_SEOLLIP, SCHOOL_INFO_SEOLLIP_DATE, SCHOOL_INFO_STUDENT_COUNT, SCHOOL_INFO_STAFF_COUNT, SCHOOL_INFO_EXERCISE_PLACE, SCHOOL_INFO_OWNER_NUMBER, SCHOOL_INFO_FAX_NUMBER, SCHOOL_INFO_OFFICE_NUMBER, SCHOOL_INFO_TEACHER_NUMBER, SCHOOL_INFO_HOMEPAGE, SCHOOL_INFO_ADDRESS, SCHOOL_INFO_CONTROL_CENTER, SCHOOL_INFO_UPDATE_DATE) values (\"{link}\",\"{school_gubun}\",\"{school_gubun2}\",\"{school_seolip_date}\",\"{school_student_cnt}\",\"{school_teacher_cnt}\",\"{school_place}\",\"{school_number}\",\"{school_fax}\",\"{school_office_number}\",\"{school_tearcher_room}\",\"{school_homepage}\",\"{school_address}\",\"{school_control}\",\"{date}\")")
      conn.commit()

  sigungu_index+=1

print('크롤링이 종료되었습니다.@@@@@@')
print('크롤링이 종료되었습니다.@@@@@@')
print('크롤링이 종료되었습니다.@@@@@@')