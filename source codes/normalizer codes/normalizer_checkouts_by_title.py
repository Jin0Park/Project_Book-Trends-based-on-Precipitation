import pandas as pd

# Read checkouts_by_title csv file
file = '/Users/seungjuchoi/Desktop/NEU/CS6240/data/Checkouts_by_Title.csv'
data = pd.read_csv(file, encoding='latin1', names=['UsageClass', 'CheckoutType', 'MaterialType', 'CheckoutYear', 'CheckoutMonth', 'Checkouts', 'Title', 'ISBN',	'Creator', 'Subjects', 'Publisher', 'PublicationYear'], dtype=str)

# Filter the data if there is duplicate Title or Null Title
data.drop_duplicates("Title", inplace=True)
data.dropna(subset=["Title"], inplace=True)

# Create a new dataframe with the columns only we need and export it as csv file.
result = data[["CheckoutYear", "CheckoutMonth", "Title", "Checkouts"]]

# Export the normalized checkouts data
result.to_csv(r'/Users/seungjuchoi/Desktop/NEU/CS6240/data/Output/Normalized_Checkouts_by_Title.csv')
