import requests
import glob
import asyncio
import re
import ujson
import uuid

import aiomysql
from aiomysql import DictCursor
from datetime import datetime,date
import csv


loop = asyncio.get_event_loop()

DATA = uuid.UUID('72a6f085-d4bf-4390-8345-5050860a7f2b')
BY_UNI = uuid.UUID('d9923e18-bd16-4ac9-895b-a9287932e1d9')


class IngestionTime:

    def __init__(self,first_file="./files/product_feed_Reebok-US_20170612.csv"):
        self.first_file = first_file
        self.files_list = glob.glob("./files/*.csv")
        self.url_dates_dict = None

    def convert_string_to_date(self,date_str):
        year = date_str[0:4]
        month = date_str[4:6]
        day = date_str[6:]
        return (year, month, day)

    def _cmd(self,*cmd):
        proto = '*{}\r\n'.format(str(len(cmd)))
        for e in cmd:
            proto += '${}\r\n'.format(len(e))
            proto += '{}\r\n'.format(e.decode('utf-8'))
        return proto

    def __convert_date_time_to_string(self,date_time):

        date_time = (date_time.strftime('%y%m%d'))
        date_time = "20" + date_time
        return date_time

    async def fetch(self,item_urls):
        conn = await aiomysql.connect(
            host='10.108.117.34',
            port=3306,
            user='krish',
            password='Vtun7EeK9Of4',
            db='findmine',
            loop=loop
        )

        cur = await conn.cursor(cursor=DictCursor)


        query = "SELECT item_url,date_created FROM items WHERE item_url IN ("
        print(query)

        for url in item_urls[:len(item_urls)-1]:
            url_value = url.replace("http","https")
            query += "'{}',".format(url_value)

        query += "'{}')".format(item_urls[len(item_urls)-1])



        print(query)

        return_dict = dict()


        await cur.execute(query)
        if(cur.rowcount!=0):
            resp = await cur.fetchall()
            print(resp)

            await conn.commit()
            await cur.close()
            conn.close()

            for value in resp:
                print(value["item_url"],self.__convert_date_time_to_string(value["date_created"]))
                return_dict[value["item_url"]] = self.__convert_date_time_to_string(value["date_created"])
            return return_dict,resp



    def create_url_list(self,path):
        total_urls = 0

        with open(path, 'r', encoding="utf8") as file:
            links_list = []
            lines = file.readlines()

            for line in lines[1:]:
                count = 0
                for field in line.split('\t'):
                    count += 1
                    if count == 9:
                        links_list.append(field)
                        total_urls += 1
                        break

            return links_list


    def create_items_first_dates(self):

        url_dates_dict = dict()
        first_urls = self.create_url_list(self.first_file)
        for first_url in first_urls:
            url_dates_dict[first_url] = list()

        for file in self.files_list:
            list_of_urls = self.create_url_list(file)

            for url in list_of_urls:
                if url_dates_dict.get(url) is None:
                    value_list = []
                    value_list.append(self.get_csv_date(file))
                    url_dates_dict[url] = value_list

                else:
                    value_list = url_dates_dict.get(url)
                    value_list.append(self.get_csv_date(file))
                    url_dates_dict[url] = value_list


        return_url_dates_dict = dict()
        for key in url_dates_dict.keys():
            return_url_dates_dict[key] = list()

        for key in url_dates_dict.keys():
            list_value = return_url_dates_dict.get(key)
            list_value.append(url_dates_dict.get(key)[0])
            return_url_dates_dict[key]=list_value




        #print(return_url_dates_dict)

        self.url_dates_dict = return_url_dates_dict
        return return_url_dates_dict


    def append_database_dates(self,limit=None,filename="batch7.csv"):

        num_not_in_db = 0
        net_difference = 0
        num_present = 0
        avg_difference = 0

        url_dates_dict = self.create_items_first_dates()
        print(url_dates_dict)
        print(url_dates_dict)
        print("Total URLs: {}".format(len(url_dates_dict.keys())))
        pass_list = list()
        for key in url_dates_dict.keys():
            pass_list.append(key)

        print(pass_list)
        value =loop.run_until_complete(self.fetch(pass_list))

        database_dates_dict = dict()
        database_dates_dict = value[0]
        #print(database_dates_dict)


        with open(filename,"w") as csvfile:
            writer_obj = csv.writer(csvfile,quoting=csv.QUOTE_ALL)
            for key in database_dates_dict.keys():
                if(database_dates_dict.get(key.replace("https","http"))=="None"):
                    num_not_in_db+=1
                else:

                    str1 = (database_dates_dict.get(key))
                    str1_date_tuple = self.convert_string_to_date(str1)
                    date_1 = date(int(str1_date_tuple[0]),int(str1_date_tuple[1]),int(str1_date_tuple[2]))

                    val1 = int(str1)

                    print(url_dates_dict.get(key.replace("https","http")))
                    str2 = url_dates_dict.get(key.replace("https","http"))[0]
                    str2_date_tuple = self.convert_string_to_date(str2)
                    date_2 = date(int(str2_date_tuple[0]),int(str2_date_tuple[1]),int(str2_date_tuple[2]))

                    val2 = int(str2)


                    num_present+=1
                    diff_date = date_1-date_2
                    diff = diff_date.days
                    print((diff))
                    net_difference += diff
                    writer_obj.writerow([key,diff])

        if(num_present!=0):
            return net_difference/num_present
        else:
            return -1






        # database_dates_dict = dict()
        #
        #
        # for values in value:
        #     for key in values.keys():
        #         database_dates_dict[key] = values.get(key)
        #
        # print(database_dates_dict)


        # if(limit is None):
        #     count=0
        #     for urls in url_dates_dict.keys():
        #         count += 1
        #         ret_value = loop.run_until_complete(self.fetch(urls))
        #         print(count,urls,ret_value)
        #         list_values = url_dates_dict.get(urls)
        #
        #         list_values.append(ret_value)
        #         url_dates_dict[urls] = list_values
        # else:
        #     count = 0
        #
        #     for urls in url_dates_dict.keys():
        #         count+=1
        #         if(count<=limit):
        #             ret_value = loop.run_until_complete(self.fetch(urls))
        #             list_values = url_dates_dict.get(urls)
        #             list_values.append(ret_value)
        #             url_dates_dict[urls] = list_values
        #         else:
        #             break
        #
        # return url_dates_dict



    def compute_and_refine_dict(self,limit=None):

        num_not_present = 0
        num_legitimate = 0
        net_difference = 0
        average_difference = 0
        url_dates_dict = self.append_database_dates(limit)
        print(url_dates_dict)
        keys_to_delete = list()


        for keys in url_dates_dict.keys():
            list_value = url_dates_dict.get(keys)
            print(list_value)
            if(len(list_value)==2 and list_value[1]=="None"):
                num_not_present+=1
                keys_to_delete.append(keys)

        for delete_key in keys_to_delete:
            url_dates_dict.pop(delete_key)

        print(url_dates_dict)

        for key in url_dates_dict.keys():
            num_legitimate += 1
            list_value = url_dates_dict.get(key)
            date_one = list_value[0]
            date_two = list_value[1]
            net_difference += abs(int(date_one)-int(date_two))

        if(num_legitimate!=0):
            average_difference = net_difference/num_legitimate

        if(num_legitimate!=0):

            return num_not_present,-1
        else:
            return num_not_present,average_difference


    def print_list_of_files(self):

        for file in self.files_list:
            print(file)


    def get_csv_date(self,file_name):
        list_values = file_name.split("/")
        name = list_values[2]
        date_csv = name.split("_")[3]
        date = date_csv.split(".")[0]

        return date





ingester = IngestionTime()
ret_value = ingester.append_database_dates()
print(ret_value)



# tuple_val = ingester.compute_and_refine_dict()
# print(tuple_val[0])
# print(tuple_val[1])

# ingester = IngestionTime()
#
#
#
#
# #print(ingester.compute_and_refine_dict()[1])
#
#
#
#
# return_val = loop.run_until_complete\
#     (ingester.
#      fetch
#      ("http://www.anrdoezrs.net/click-7654620-11945161-1432894004053?"
#       "url=http%3A%2F%2Fwww.mangooutlet.com%2Fredirect.faces%3Fop%3Dconta%26"
#       "idioma%3DUS%26pais%3D400%26producto%3D31077547%26color%3D02&"
#       "cjsku=310775474850"))
# print(type(return_val))
# return_val = (return_val.strftime('%y%m%d'))
# return_val = "20" + return_val
# print(return_val)










