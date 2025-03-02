import requests
import csv
import os
import pandas as pd

def main():

    for i in range(1946, 2024):
        season_id = '{}-{}'.format(str(i), str(i+1)[-2:])

        url = 'https://stats.nba.com/stats/leaguegamelog?Counter=1000&DateFrom=&DateTo=&Direction=DESC&LeagueID=00&PlayerOrTeam=T&Season='+season_id+'&SeasonType=Regular%20Season&Sorter=DATE'
        headers  = {
            'Connection': 'keep-alive',
            'Accept': 'application/json, text/plain, */*',
            'x-nba-stats-token': 'true',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
            'x-nba-stats-origin': 'stats',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Referer': 'https://stats.nba.com/',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
        }

        response = requests.get(url=url, headers=headers).json()
        games = response['resultSets'][0]['rowSet']
        
        header = response['resultSets'][0]['headers']
        data_file_name = '{}.csv'.format(season_id)
        data_file = open(data_file_name, 'w')
        
        csv_writer = csv.writer(data_file)
        
        count = 0
        
        for game in games:
            if count == 0:
        
                header = header
                csv_writer.writerow(header)
                count += 1
        
            csv_writer.writerow(game)
        
        data_file.close()

    csv_folder = '/home/xh0i/dsc650-infra/bellevue-bigdata/nifi/nifi-1.25.0/weeks_11_12'

    combined_data = pd.DataFrame()

    for filename in os.listdir(csv_folder):
        if filename.endswith('.csv'):
            filepath = os.path.join(csv_folder, filename)
            df = pd.read_csv(filepath)
            combined_data = pd.concat([combined_data, df], ignore_index=True)

    output_file = '/home/xh0i/dsc650-infra/bellevue-bigdata/nifi/nifi-1.25.0/weeks_11_12/all_games.csv'
    combined_data.to_csv(output_file, index=False)

main()    