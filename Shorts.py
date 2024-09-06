# Function to calculate day count excluding weekends

def calculate_day_count(start_date, end_date):
    df = pd.DataFrame(pd.date_range(start=start_date, end=end_date), columns=['Date'])
    day_count = 1
    for index, row in df.iterrows():
        day_of_week = row['Date'].dayofweek
        if day_of_week < 5:
            df.loc[index, 'day_count'] = day_count
            day_count += 1
    return df


  #SQL connection by SQL alchemy : push the dataframe to SQL


