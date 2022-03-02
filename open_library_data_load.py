import wget

url = "https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump.json"
path = "C:\\Users\\user\\PycharmProjects\\MySparkSetup\\data\\raw_data"
filename = wget.download(url, out = path)