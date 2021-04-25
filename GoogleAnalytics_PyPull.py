import pandas as pd
from apiclient.discovery import build
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import socket
import pandas_gbq
from google.cloud import bigquery

# Authentication for Google Analytics & BigQuery API calls

scope = 'https://www.googleapis.com/auth/analytics.readonly'
credentials = service_account.Credentials.from_service_account_file('/Users/Tanner/utils/googlecloud_auth.json')
service = build('analytics', 'v3', credentials=credentials)



# Date variables -- Used for specifying date range of data pull
today = datetime.now().date()
yesterday = today - timedelta(days=1)



def analytics_api_query(client_id, index, start_date, end_date, dimensions, metrics): #builds query pull
	
	# 7 dimensions, 10 metrics limit
	data = service.data().ga().get(
		ids='ga:' + client_id,
		start_date=start_date,
		end_date=end_date,
		metrics=metrics,
		dimensions=dimensions,
		start_index=index,
		max_results=10000).execute()

	return data



def analytics_get_report(client_id, start_date, end_date, dimensions, metrics): #grabs data
	
	index = 1
	totalResults = 2
	output = []

	while totalResults > index:
		a = analytics_api_query(client_id, index, start_date, end_date, dimensions, metrics)
		totalResults = a.get('totalResults')
		index = index + 10000

		try:
			for row in a.get('rows'):
				output.append(row)
		except:
			pass

	return output



def main(client_id):

	base_df = pd.DataFrame()
	columns = traffic_columns
	start_date = '2019-01-01'
	start_date = datetime.strptime(start_date , '%Y-%m-%d').date()

	print('Running Google Analytics Report.')
	
	
	#change based the type and number of dimensions pulled. These are set 
	dimensions = 'ga:'+(',ga:'.join(columns[:7]))
	metrics = 'ga:'+(',ga:'.join(columns[7:]))
	
	results = analytics_get_report(client_id, str(start_date), str(yesterday), dimensions, metrics)
	df = pd.DataFrame(results, columns=columns)

	df['view_id'] = client_id
	base_df = base_df.append(df)

	base_df = base_df.applymap(str)
	base_df = base_df[base_df.date != '(other)'] #remove if date column has '(other)' as a value
	base_df['date'] = pd.to_datetime(base_df['date'], format='%Y%m%d') #format date
	
	# Powershell output
	print(base_df.head())

	# csv output
	base_df.to_csv('/Users/Tanner/Desktop/example-export-ga.csv') #change to desired file path / file name

	#bigquery output
	base_df.to_gbq(
		'raw_data.t_googleanalytics_data', #dataset name + table name
		'client-test-275515', #project name
		chunksize=10000,
		reauth=False,
		if_exists='replace',
		credentials=credentials
		)

'''
visit https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/ to understand metrics vs dimensions and 
what can /can't be pulled together
'''

traffic_columns = [
	'date', #dimension
	'source', #dimension
	'sourceMedium', #dimension
	'userType', #dimension
	'campaign', #dimension
	'keyword', #dimension
	'adContent', #dimension
	'sessions', #metric
	'users', #metric
	'bounces', #metric
	'sessionDuration', #metric
	'goalCompletionsAll', #metric
	'transactions', #metric
	'transactionRevenue' #metric
	] 



main('your_GA_ID') #GA View Id
