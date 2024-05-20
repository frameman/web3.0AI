from dotenv import load_dotenv 
import os
load_dotenv()
eth_api_key = os.getenv("etherscan_api_key")
import csv
import requests
import json 
import itertools
#read the json file get from the github
def read_data():

    json_file = "<github file/.json path>"

    with open(json_file,"r") as file:
        data = json.load(file)

    #get the tokens attribute
    tokens = data["tokens"]
    tokenlist = [[token] for token in tokens]
    flattened_list = list(itertools.chain.from_iterable(tokenlist))
    
    for i in range(len(flattened_list)):
        print(flattened_list[i])
        get_transactions(flattened_list[i],i)


##Get a list of 'Normal' Transactions By Address##
def get_transactions(address:str,i)->str:
    i = i
    url_target:str = "https://api.etherscan.io/api"
    module:str = "account"
    address:str = address
    action:str = "txlist"
    startblock:int = 0
    endblock:int = 99999999
    page:int = 1
    offset:int = 100
    sort:str = "asc"

    url = ( f'{url_target}'
            f"?module={module}"
            f"&action={action}"
            f"&apikey={eth_api_key}"
            f"&address={address}"
            f"&startblock={startblock}"
            f"&endblock={endblock}"
            f"&page={page}"
            f"&offset={offset}"
            f"&sort={sort}")
    r = requests.get(url)
    json_result = json_parse(r.text)
    csv_file(json_result,i)

##parse the json file##
def json_parse(re_data) ->json:
    try:
        # 尝试解析JSON字符串
        parsed_json = json.loads(re_data)
    except json.JSONDecodeError as e:
        # 如果解析失败，打印错误并返回
        print(f"Error parsing JSON: {e}")
        return
    return parsed_json


# write the json file to csv file
def csv_file(_data_,k):
    file_name = "<Path to save the csv file>"
    #file_name = f"C:\\Users\\...\\csvfile\\{k}_transactions.csv"
    with open(file_name,"w",newline="") as file:
        fieldnames = _data_["result"][0].keys()
        writer = csv.DictWriter(file,fieldnames=fieldnames)
        writer.writeheader()
        for i in _data_["result"]:
            writer.writerow(i)
    print(k)
 



if __name__ == "__main__":
    read_data()
