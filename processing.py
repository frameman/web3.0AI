import pandas as pd
import os
def start(df):
    """
    remove the columns that are not needed,and fillun all the NaN value with 0
    """
    df =df
    column = {'hash', 'blockNumber', 'blockHash',"input","nonce","contractAddress","methodId","transactionIndex","to_frequency","from_frequency","blockNumber"}
    for j in column:
                if j in df.columns:
                    df = df.drop(columns=[j])
    df = df.fillna(0)
    return df
    #print(df.isna())

def end(df):
    """
    remove the colums of to from timStamp and datetime,
    to, from is the address of wallet which can't be used to process from the connection for now
    datetime is the covert of timestamp and it already use to count as the time frequency of functionName, frequncey of sending to the same 
    """
    df =df
    column = {"datetime","to","from","timeStamp"}
    for j in column:
                if j in df.columns:
                    df = df.drop(columns=[j])
    return df



"""
The time period in seconds
    5 minutes = 300 seconds
    20 minutes = 1200 seconds
    60 minutes = 3600 seconds
    2 hours = 7200 seconds
    5 hours = 18000 seconds
    10 hours = 36000 seconds
    24 hours = 86400 seconds
    2 days = 172800 seconds
    5 days = 432000 seconds
    10 days = 864000 seconds
    30 days = 2592000 seconds
"""
def update_time_count(df, seconds_list):
    """
    Analyze transaction data to check if a particular address reappears within specified number of days and update the DataFrame.
    
    :param df: DataFrame containing transaction data, must include 'timeStamp', 'from', 'to' columns
    :param days_list: List of specified time periods in days
    :return: Updated DataFrame with repeat transaction flags and frequencies
    """
    # Convert 'timeStamp' column to datetime type and sort by 'from', 'to', 'timeStamp'
    df = df.copy()
    df['datetime'] = pd.to_datetime(df['timeStamp'], unit='s')
    df = df.sort_values(by=['from', 'to', 'datetime'])
    
    # Get the minimum and maximum dates in the dataset
    min_date_in_dataset = df['datetime'].min()
    max_date_in_dataset = df['datetime'].max()

    # Loop through each specified time period
    for seconds in seconds_list:
        max_date = max_date_in_dataset
        min_date = max_date - pd.Timedelta(seconds=seconds)
        repeat_col_name = f'transaction_repeat_within_{seconds}_seconds'
        frequency_col_name = f'transaction_repeat_within_{seconds}_frequency'
        df[repeat_col_name] = 0
        df[frequency_col_name] = 0

        print(f"Processing for {seconds} seconds: from {min_date} to {max_date}")
        # Initialize the repeat transaction flag column
        # Group by 'from' and 'to' and check for repeat transactions within the specified period
        condition = (df['datetime'] >= min_date) & (df['datetime'] < max_date)
        df.loc[condition, repeat_col_name] = 1
        # Fill NaN values in the repeat column (if any) and convert to integer
        df[repeat_col_name] = df[repeat_col_name].fillna(0).astype(int)
        # Calculate the frequency of repeat transactions
        df[frequency_col_name] = (
            df.groupby(['from', 'to'])[repeat_col_name]
              .transform('sum')
              .fillna(0)
              .astype(int)
        )
    return df


#using one hot encoding to change the functionName to number
def functionName(df):
    transactions_df = df
    transactions_df_encoded = pd.get_dummies(transactions_df, columns=['functionName'], prefix='function', dtype=int)
    return transactions_df_encoded

#i only want to connect from to function_name and repeat_within secound_frequency columns to calulate the frequency 
# connection all the columns start with func
def functionName_fre(df,seconds_list):
    df =df.copy()
    function_columns = [col for col in df.columns if col.startswith('function_')]
    for seconds in seconds_list:
        for col in function_columns:
            df[f'transaction_repeat_function_within_{seconds}_frequency']=df[f'transaction_repeat_within_{seconds}_frequency'] = (
            df.groupby(['from', 'to', col])[f'transaction_repeat_within_{seconds}_seconds']
              .transform('sum')
              .fillna(0)
              .astype(int)
            )
            
    return df
        
   

def main():
    seconds =[1,300,1200,3600,7200,18000,36000,86400,172800,432000,864000,2592000]
    #there is 761 csv file start count from 0
    for id in range(761) :
        #e.g file = f"C:\\Users\\...\csvfile\\{id}_transactions.csv"
        file  = "<Path of csvfile>"
        #e.g save_file = f"C:\\Users\\...\\csvfile2\\{id}_transactions.csv"
        save_file ="<Path for saving the file>"
        
        if os.path.getsize(file) > 0:
            try:
                df = pd.read_csv(file)
            except pd.errors.EmptyDataError:
                print("The file is empty or has no columns.")
        else:
            print(f"The {id}_file is empty.")
        
        star = start(df)
        up = update_time_count(star,seconds)
        #print(up.columns)
        #up.to_csv(save_file,index=False)
        func =functionName(up)
        more =functionName_fre(func,seconds)
        endl=end(more,file)
        endl.to_csv(save_file,index=False)

        print(id," finish")
if __name__ == '__main__':
    main()
