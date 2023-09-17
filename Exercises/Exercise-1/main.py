import requests
import os
from zipfile import ZipFile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():
# 1. create the directory `downloads` if it doesn't exist
# 2. download the files one by one.
# 3. split out the filename from the uri, so the file keeps its 
#    original filename.
   
# 4. Each file is a `zip`, extract the `csv` from the `zip` and delete
# the `zip` file.
# 5. For extra credit, download the files in an `async` manner using the 

#    `Python` package `c`. Also try using `ThreadPoolExecutor` in 
#    `Python` to download the files. Also write unit tests to improve your skills.

    # Path of the folder where the user want to download the Zips
    path="downloads/"
    # 1- Create the folder if does not exist
    os.makedirs(path,exist_ok=True)
    # 2- Create a loop for each uri
    for eachuri in download_uris:
        # This try except is because 1 of them uris is invalid, 
        # in my case I use try except but the user can modify by his way
        try:
            # 3- Call request to download each uri
            Call_api = requests.get(eachuri)
            # 4- Check the status = 200 .... if the satatus = 400 is wrong or failed download
            Call_api.status_code
            # 5- split the name of the file of each uri to get the original name
            #  example: Divvy_Trips_2220_Q1.zip  for this we should split the last "/"
            OriginalNameZip = eachuri.split("/")[-1]
            # 6- Join the path and the name -> "downloads/Divvy_Trips_2220_Q1.zip"
            FilePathZip = os.path.join(path,OriginalNameZip)
            print("Relative Path of File: ",FilePathZip)
            # 6- Open the folder where the user can donwload the ZIP
            with open(FilePathZip,'wb') as folder:
                # 7- Write the download content into the folder
                folder.write(Call_api.content)
                # It is important close the function!
                folder.close()
            # 8- Unzip the Zip file and we can extract the csv with the function "extractall"
            with ZipFile(FilePathZip,'r') as unzip:
                # 9- Unzip the file and write where they are going to save
                unzip.extractall(path=path)
                # Important close the function
                unzip.close()
            # 10- Remove all Zip files, with function remove(name of the files) the user can remove them easily
            os.remove(FilePathZip)
        except Exception as e:
            os.remove(FilePathZip)
            print("an uri could not be download: ", e)
if __name__ == "__main__":
    main()

