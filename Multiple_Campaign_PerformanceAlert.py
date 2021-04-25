from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import traceback
import smtplib
from google.oauth2 import service_account
from google.cloud import bigquery

### Google BigQuery Access ###

credentials = service_account.Credentials.from_service_account_file('{INSERT SERVICE ACCOUNT JSON FILE}') #file path where .json is located

bigquery_client = bigquery.Client(
		credentials=credentials, 
		project=credentials.project_id)


def googleads_performance_check():

	print('Running Data for Google Ads')

	sql = """
 		 SELECT 
      	  campaignName,
		  ninetyday_impressions,
		  ninetyday_clicks,
		  ninetyday_cost,
		  ninetyday_conversions,
		  sevenday_impressions,
		  sevenday_clicks ,
		  sevenday_cost,
		  sevenday_conversions,
		  (ninetyday_clicks/ninetyday_impressions) AS ninetyday_ctr,
		  (sevenday_clicks/sevenday_impressions) AS sevenday_ctr,
		  (nullif(ninetyday_cost,0)/nullif(ninetyday_conversions,0)) AS ninetyday_cpa,
		  (nullif(sevenday_cost,0)/nullif(sevenday_conversions,0)) AS sevenday_cpa,
		  ((ninetyday_cost/ninetyday_impressions)*1000) AS ninetyday_cpm,
		  ((sevenday_cost/sevenday_impressions)*1000) AS sevenday_cpm,
		  (nullif(sevenday_conversions,0)/nullif(sevenday_impressions,0)) AS sevenday_cvr,
		  (nullif(ninetyday_conversions,0)/nullif(ninetyday_impressions,0)) AS ninetyday_cvr
		  FROM
		  `{ADD PROJECT_ID}.{ADD DATASET_NAME}.{ADD TABLE_NAME}`
      	  WHERE sevenday_clicks > 0 and sevenday_impressions > 150 and sevenday_cost > 500 # Change based on your preferred criteria or remove altogether
			"""

	job_config = bigquery.QueryJobConfig()
	job_config.use_legacy_sql = False

	performance = bigquery_client.query(sql, job_config=job_config).result().to_dataframe()

	### CTR Performance Check by Campaign ###

	ctr_threshold = 0.10 # Set based on your preferred criteria

	for index, row in performance.iterrows():
		
		print(str(row['campaignName'])+' | 90 Day CTR: '+str(row['ninetyday_ctr'])+' | 7 Day CTR: '+str(row['sevenday_ctr']))

		if row['ninetyday_ctr']*(1-ctr_threshold) > row['sevenday_ctr']:
			print('CTR Perfromance Alert Triggered')
			
			# Email Formatting and Message Creation
			body_header = "In the past seven days, Campaign CTR is down at least "+str('{0:.0%}'.format(ctr_threshold))+" compared to the 90 day average "
			body_detail = 'Task Details:'+"<br>"+"Campaign Ninety Day CTR: "+str('{:.2%}'.format(row['ninetyday_ctr']))+"<br>"+"Campaign Seven Day CTR: "+ str('{:.2%}'.format(row['sevenday_ctr']))
			body_footer = "Check in to perform any optmizations necessary"     

			full_trace = traceback.format_exc()
			fromaddr = "{ADD AUTHENTICATED EMAIL ADDRESS}" # The email address authenticated to send messages
			toaddr = ['{ADD EMAIL ADDRESS TO SEND ALERTS TOO'] # The email addresses to send alerts to. In list format (separated by commas)
			msg = MIMEMultipart()
			msg['From'] = fromaddr
			msg['To'] = ", ".join(toaddr)
			msg['Subject'] = "Google Ads CTR Performance Alert | "+row['campaignName']

			html = """
			<html>
			  <head></head>
			  <body>
				{body_header}<br><br>
				{body_detail}<br><br>
				{body_footer}
			  </body>
			</html>
			""".format(body_header=body_header, body_detail=body_detail, body_footer=body_footer)

			part1 = MIMEText(html, 'html')
			msg.attach(part1)
			server = smtplib.SMTP('smtp.gmail.com', 587) # change based on whatever email system used (outloook would be smtp.live.com)
			server.starttls()
			server.login(fromaddr, "{ADD_EMAIL_OR_APP_PASSWORD") # App password needed if your email address has 2-step authentication
			text = msg.as_string()
			server.sendmail(fromaddr, toaddr, text)
			server.quit()
			print('Alert sent via email')
		

googleads_performance_check()

"""
This is the sample query for data pull in GCP Bigqueryy

#1
SELECT
  CAST(date as date) as date,
  CampaignName,
  CAST(impressions as float64) AS impressions,
  CAST(clicks as float64) AS clicks,
  CAST(cost as float64) AS cost,
  CAST(conversions as float64) AS conversions
FROM
  `client-test-275515.raw_data.t_googleads_campaigns`
  
  
#2
SELECT
  CampaignName,
  SUM(impressions) AS ninetyday_impressions,
  SUM(clicks) AS ninetyday_clicks,
  SUM(cost) AS ninetyday_cost,
  SUM(conversions) AS ninetyday_conversions,
  SUM(CASE
      WHEN date >= CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY) AS date) AND date <= CAST(CURRENT_DATE() AS date) THEN impressions
    ELSE
    0
  END
    ) AS sevenday_impressions,
  SUM(CASE
      WHEN date >= CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY) AS date) AND date <= CAST(CURRENT_DATE() AS date) THEN clicks
    ELSE
    0
  END
    ) AS sevenday_clicks,
  SUM(CASE
      WHEN date >= CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY) AS date) AND date <= CAST(CURRENT_DATE() AS date) THEN conversions
    ELSE
    0
  END
    ) AS sevenday_conversions,
  SUM(CASE
      WHEN date >= CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY) AS date) AND date <= CAST(CURRENT_DATE() AS date) THEN cost
    ELSE
    0
  END
    ) AS sevenday_cost
FROM
  `client-test-275515.raw_data.v_googleads_campaigns`
WHERE
  date >= CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -90 DAY) AS date)
  AND date <= CAST(CURRENT_DATE() AS date)
GROUP BY
  1
"""
