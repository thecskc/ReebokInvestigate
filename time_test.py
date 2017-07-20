import glob

class TimeTest:


    def __init__(self,first_file="./files/product_feed_Reebok-US_20170426.csv"):
        self.first_file = first_file
        self.files_list = glob.glob("./files/*.csv")
        self.url_dates_dict = None

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

    async def fetch(self,item_url):
        conn = await aiomysql.connect(
            host='10.108.117.34',
            port=3306,
            user='krish',
            password='Vtun7EeK9Of4',
            db='findmine',
            loop=loop
        )

        cur = await conn.cursor(cursor=DictCursor)
        item_url = item_url.replace("http","https")
        query = "SELECT date_created FROM items WHERE item_url = '{}'".format(item_url)
        #print(query)


        await cur.execute(query)
        if(cur.rowcount!=0):
            resp = await cur.fetchall()


            await conn.commit()
            await cur.close()
            conn.close()

            #print(type(resp))
            for prod in resp:
                return(prod['date_created'])
        else:
            return "None"




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



    def read_file(self,file="txtfile"):
        dict_values = dict()

        with open(file,"r") as f_obj:
            for line in f_obj:
                list = line.split(" ")
                time_list = list[2].split("-")
                time_str=""
                for time_val in time_list:
                    time_str+=time_val.strip()



                dict_values[list[1]] = time_str

        print(dict_values)

        return dict_values

    def print_list_of_files(self):

        for file in self.files_list:
            print(file)

    def get_csv_date(self, file_name):
        list_values = file_name.split("/")
        name = list_values[2]
        date_csv = name.split("_")[3]
        date = date_csv.split(".")[0]

        return date

    def compute_difference_and_missing_values(self):
        url_dates_dict = self.create_items_first_dates()
        file_urls = self.read_file()

        missing_values = 0
        net_difference = 0
        proper_values  = 0

        for keys in url_dates_dict.keys():
            ret_value = file_urls.get(keys)
            if(ret_value=="None"):
                missing_values +=1
            else:


                con_value = int(url_dates_dict.get(keys)[0])

                print(con_value,int(ret_value),con_value-int(ret_value))
                net_difference += abs(con_value - int(ret_value))
                proper_values +=1

                # if(int(ret_value)>=con_value):
                #     net_difference+=abs(con_value-int(ret_value))
                #     proper_values +=1


        print(proper_values,missing_values)
        if(proper_values!=0):
            return net_difference/proper_values,missing_values
        else:
            return -1,missing_values







time = TimeTest()
time.read_file()
print(time.create_items_first_dates())

ret = time.compute_difference_and_missing_values()
print(ret[0],ret[1])


