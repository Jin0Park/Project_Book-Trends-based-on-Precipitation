import pandas as pd

# Imports the csv file we want to modify and set the dataframe with 7 columns we needed.
file = '/Users/seungjuchoi/Desktop/NEU/CS6240/CS6240_Project/data/book32-listing.csv'
data = pd.read_csv(file, encoding='latin1', names=['AmazonISBN', 'Filename', 'ImageURL', 'Title', 'Author', 'CategoryID', 'Category'], dtype=str)

# Iterates the csv data we imported, then check each row if this row's ISBN is less than 10.
# If it is shorter than 10, we add correct 0s to the front of ISBN so it has 10 characters.
for index, row in data.iterrows():
    if len(data.loc[index, 'AmazonISBN']) < 10:
        num_zeros = 10 - len(data.loc[index, 'AmazonISBN'])
        zeros_to_add = "0" * num_zeros
        data.at[index, 'AmazonISBN'] = str("\"" + zeros_to_add + data.loc[index, 'AmazonISBN'] + "\"")
    else:
        data.at[index, 'AmazonISBN'] = str("\"" + data.loc[index, "AmazonISBN"] + "\"")

# Exports the modified CSV file to the local path.
data.to_csv(r'/Users/seungjuchoi/Desktop/NEU/CS6240/CS6240_Project/data/normalized_book32_listing.csv', header=True)

# Prints the success message after CSV file exportation.
print("Modified CSV file is exported successfully!")
