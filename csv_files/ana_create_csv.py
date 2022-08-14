import pandas as pd
df =pd.read_csv('/home/kimia/Desktop/new/websites/csv_files/ana.csv')
base_url = "https://www.ana.press/archive?pi=1&ms=0&dy=31&mn=3&yr=1397"
urls = []
# news start from 1393
for year in range(1393,1401):
    for month in range(1,13):
        for day in range(1,32):
            for page in range(1,15):
                url = base_url.replace("yr=1397","yr="+str(year))
                url = url.replace("mn=3","mn="+str(month))
                url = url.replace("pi=1","pi="+str(page))
                url = url.replace("dy=31","dy="+str(day))
                print(url)
                urls.append(url)
print(len(urls))
for url in urls:
    df.loc[len(df.index)] = [url,'ana']
df.to_csv('/home/kimia/Desktop/new/websites/csv_files/ana.csv',index=False)