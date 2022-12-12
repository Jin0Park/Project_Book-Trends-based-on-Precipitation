import pandas as pd

# Read checkouts_by_title csv file
file = "/Users/seungjuchoi/Desktop/NEU/CS6240/data/Original/Library_Collection_Inventory.csv"
data = pd.read_csv(file, encoding='latin1', names=['BibNum', 'Title', 'Author', 'ISBN', 'PublicationYear', 'Publisher', 'Subjects', 'ItemType', 'ItemCollection', 'FloatingItem', 'ItemLocation', 'ReportDate', 'ItemCount'], dtype=str)

# Create a new dataframe with only Title and ISBN columns
title_isbn_only = data[["Title", "ISBN"]]

# Filter the data by removing the rows with null ISBN or TItle, duplicate Title.
title_isbn_only.drop_duplicates("Title", inplace=True)
title_isbn_only.dropna(subset=["Title"], inplace=True)
title_isbn_only = title_isbn_only[title_isbn_only.ISBN != "\"nan\""]
# Filter the lambda equation using apply function to filter out the rows have multiple ISBNs to pick the only first ISBN which has only 10 digits.
title_isbn_only["ISBN"] = title_isbn_only["ISBN"].apply(lambda x: "\"" + str(x)[:10] + "\"" if len(str(x)) > 10 else "\"" + str(x) + "\"")

# Export the normalized data 
title_isbn_only.to_csv(r'/Users/seungjuchoi/Desktop/NEU/CS6240/data/Output/Normalized_Library_Inventory_Collection.csv')
