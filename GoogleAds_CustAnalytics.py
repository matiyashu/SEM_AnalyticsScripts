# Mandatory module imports
import io
import json
import pandas as pd
from googleads import adwords
from datetime import datetime, date, time, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import pandas_gbq
from google.cloud import bigquery


# API version
version = 'v201809' # Insert current api version

# Authentication
googleads_client = adwords.AdWordsClient.LoadFromStorage('{INSERT GOOGLE ADS YAML FILE}')
credentials = service_account.Credentials.from_service_account_file('{INSERT SERVICE ACCOUNT JSON FILE}') #file path where .json is located

# Date variables
today = datetime.now().date()
yesterday = today - timedelta(days=1)
thirty_days_ago = today - timedelta(days=30)


def googleads_report(client_id, report_type, columns, start_date, end_date):

	googleads_client.SetClientCustomerId(client_id)
	report_downloader = googleads_client.GetReportDownloader(version=version)

	report = {
		'reportName': 'report-google-campaign-performance',
		'dateRangeType': 'CUSTOM_DATE',
		'reportType': report_type,
		'downloadFormat': 'CSV',
		'selector': {
			'fields': columns,
			'dateRange': {'min':start_date,'max':end_date}
			}
		}

	file = io.StringIO(report_downloader.DownloadReportAsString(
		report,
		skip_report_header=True,
		skip_column_header=True,
		skip_report_summary=True,
		include_zero_impressions=False)
		)

	df = pd.read_csv(file, names=columns)
	return df


def main(client_id):

	report_types = [
		'CAMPAIGN_PERFORMANCE_REPORT',
		'KEYWORDS_PERFORMANCE_REPORT',
		'AD_PERFORMANCE_REPORT'
		]

	for report in report_types:
		base_df = pd.DataFrame()
		if report == 'CAMPAIGN_PERFORMANCE_REPORT':
			table_suffix = 'campaigns'
			columns = campaign_columns
		elif report == 'KEYWORDS_PERFORMANCE_REPORT':
			table_suffix = 'keywords'
			columns = keyword_columns
		elif report == 'AD_PERFORMANCE_REPORT':
			table_suffix = 'ads'
			columns = ad_columns


		start_date = '2021-01-01'

		df = googleads_report(client_id, report, columns, start_date, yesterday)
		df = df.applymap(str)	
		
			
		
		# Powershell output
		print(df.head())

		# csv output
		df.to_csv('/Users/Tanner/Desktop/example-export-googleads-'+table_suffix+'.csv') #change to desired file path

		#bigquery output
		
		df.to_gbq(
			'raw_data.t_googleads_'+table_suffix, #dataset name.table name
			'{INSERT PROJECT ID}', #project id
			chunksize=10000,
			reauth=False,
			if_exists='replace', #change to 'append' if you want existing table to remain and new data added to bottom
			credentials=credentials
			)


campaign_columns = [
	'Date',
	'DayOfWeek',
	'Device',
	'AccountDescriptiveName',
	'AdvertisingChannelType',
	'AdvertisingChannelSubType',
	'Amount',
	'BaseCampaignId',
	'CampaignName',
	'CampaignStatus',
	'CustomerDescriptiveName',
	'ExternalCustomerId',
	'Labels',
	'ServingStatus',
	'Clicks',
	'Conversions',
	'Cost',
	'ConversionValue',
	'GmailForwards',
	'GmailSaves',
	'GmailSecondaryClicks',
	'Impressions',
	'VideoViews',
	'ViewThroughConversions'
	]

keyword_columns = [
	'Date',
	'AccountDescriptiveName',
	'AdGroupId',
	'AdGroupName',
	'AdGroupStatus',
	'CampaignId',
	'CampaignName',
	'CampaignStatus',
	'CpcBid',
	'Criteria',
	'CriteriaDestinationUrl',
	'ExternalCustomerId',
	'FirstPageCpc',
	'FirstPositionCpc',
	'Id',
	'KeywordMatchType',
	'Labels',
	'QualityScore',
	'SearchImpressionShare',
	'Status',
	'TopOfPageCpc',
	'Clicks',
	'Conversions',
	'Cost',
	'ConversionValue',
	'Impressions',
	'ViewThroughConversions'
	]

ad_columns = [
	'Date',
	'AccountDescriptiveName',
	'AdGroupId',
	'AdGroupName',
	'AdGroupStatus',
	'AdType',
	'CampaignId',
	'CampaignName',
	'CreativeFinalUrls',
	'CriterionType',
	'Description',
	'Description1',
	'Description2',
	'ExternalCustomerId',
	'Headline',
	'HeadlinePart1',
	'HeadlinePart2',
	'Id',
	'ImageCreativeImageHeight',
	'ImageCreativeImageWidth',
	'Labels',
	'LongHeadline',
	'Path1',
	'Path2',
	'ShortHeadline',
	'Status',
	'Clicks',
	'Conversions',
	'Cost',
	'ConversionValue',
	'Impressions',
	'ViewThroughConversions'
	]


			
main('{INSERT GOOGLE ADS CUSTOMER ID}')
