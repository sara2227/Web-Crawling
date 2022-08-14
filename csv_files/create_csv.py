import pandas as pd

base_url = "https://www.mehrnews.com/archive?pi=1&ms=0&dy=1&mn=1&yr=1390"
df = pd.read_csv('/home/kimia/Desktop/new/websites/csv_files/mehrnews.csv')
urls = []
for year in range(1390, 1401):
    for month in range(1, 13):
        for day in range(1, 31):
            if year == 1390 and month == 1 and day in range(1, 6):
                continue
            for page in range(1, 29):
                url = base_url.replace("yr=1390", "yr=" + str(year))
                url = url.replace("mn=1", "mn=" + str(month))
                url = url.replace("pi=1", "pi=" + str(page))
                url = url.replace("dy=1", "dy=" + str(day))
                print(url)
                urls.append(url)

print(len(urls))
for url in urls:
    df.loc[len(df.index)] = [url, "mehrnews"]
df.to_csv('/home/kimia/Desktop/new/websites/csv_files/mehrnews.csv', index=False)
