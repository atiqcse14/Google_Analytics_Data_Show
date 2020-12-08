from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'C:/Users/progoti/PycharmProjects/GA_Project/venv/client_secret.json'
VIEW_ID = '184319431'

def initialize_analyticsreporting():

  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics

def get_report(analytics):

  rgx='~\?.*'

  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': '30daysAgo', 'endDate': 'yesterday'}],
          'metrics': [{'expression': 'ga:sessions'}],
          'dimensions': [{'name': 'ga:pagePath'}],
          'filtersExpression':f'ga:pagePath={rgx}', 
          'orderBys': [{"fieldName": "ga:sessions", "sortOrder": "DESCENDING"}], 

        }]
      }
  ).execute()

def save_response(response):

  _dimension = []
  _value = []

  for report in response.get('reports', []):

    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

    for row in report.get('data', {}).get('rows', []):
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])

      for header, dimension in zip(dimensionHeaders, dimensions):
        _dimension.append(dimension)

      for i, values in enumerate(dateRangeValues):
        for metricHeader, value in zip(metricHeaders, values.get('values')):
            _value.append(value)

    _data = pd.DataFrame() 
    _data["Sessions"]=_value
    _data["pagePath"]=_dimension
    _data=_data[["pagePath","Sessions"]]

    _data.to_csv("parameter_pages.csv")

def main():
  analytics = initialize_analyticsreporting()
  response = get_report(analytics)
  print(response)
  save_response(response)

if __name__ == '__main__':
  main()