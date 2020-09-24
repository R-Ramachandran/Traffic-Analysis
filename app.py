from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import dash
from dash.dependencies import Output, Input
import dash_core_components as dcc
import dash_html_components as html
import plotly
import random
import plotly.graph_objects as go
from collections import deque
from plotly import subplots
import pandas as pd
import numpy as np
from datetime import datetime
import os



import psycopg2

# DATABASE_URL = os.environ['DATABASE_URL']			for Heroku Hosting Purpose

column_names_real_time_geo = ['country', 'region', 'city', 'longitude', 'latitude', 'medium', 'source', 'users', 'text']
column_names_overview_geo = ['country', 'region', 'city', 'longitude', 'latitude', 'users', 'sessions', 'UniquePageviews', 'bounceRate', 'avgSessionDuration', 'hits', 'text']
column_names_source_geo = ['country', 'region', 'city', 'longitude', 'latitude', 'source','users', 'text']
column_names_medium_geo = ['country', 'region', 'city', 'longitude', 'latitude', 'medium','users', 'text']
column_names_bandwidth = ['pageviews','users', 'date', 'text']
column_names_os = ['operatingSystem', 'users','date','text']
column_names_browser = ['browser', 'users','date','text']
column_names_device = ['deviceCategory', 'users','date','text']
column_names_sessions = ['sessions', 'bounceRate', 'hits','date','text']
column_names_pageviews = ['pageviews','pageviewsPerSession','uniquePageviews','avgTimeOnPage','date','text']
column_names_users = ['visitorType','users']
column_names_overall = ['Users','Sessions','Avg. Session Duration','Pageviews','Pageviews Per Session','Bounce Rate','Avg. Time On Page','Hits', 'Unique Pageviews']

df = pd.DataFrame(columns=column_names_real_time_geo)

def get_service(api_name, api_version, scopes, key_file_location):
	credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location, scopes=scopes)
	service = build(api_name, api_version, credentials=credentials)
	return service

def get_first_profile_id(service):
	accounts = service.management().accounts().list().execute()
	if accounts.get('items'):
		account = accounts.get('items')[0].get('id')
		properties = service.management().webproperties().list(
				accountId=account).execute()
		if properties.get('items'):
			property = properties.get('items')[0].get('id')
			profiles = service.management().profiles().list(accountId=account,webPropertyId=property).execute()
			if profiles.get('items'):
				return profiles.get('items')[0].get('id')
	return None

def get_results(service, profile_id):
	return service.data().realtime().get(
				ids='ga:' + profile_id,
				metrics='rt:activeUsers',
				dimensions= 'rt:country, rt:region, rt:city, rt:longitude, rt:latitude, rt:medium, rt:source'
			).execute()

def get_plot(df, col):
	plots = subplots.make_subplots(
		rows=1, cols=1,
		specs=[[{'type':'scattergeo'}]]
	)
	plots.add_trace(go.Scattergeo(
		lon = df['longitude'],
		lat = df['latitude'],
		text = df['text'],
		mode = 'markers',
		marker = dict(size = (np.array(df['users'].astype(int))*10).tolist(), color = df[col].astype(float)/sum(df[col].astype(float))*100)
	))
	plots.update_layout(
		geo = dict(
			showland = True,
			landcolor = "#60d952",
			showocean = True,
			oceancolor = "#80D0FF",
			subunitcolor = "orange",
			countrycolor = "black",
			showlakes = False,
			lakecolor = "lightblue",
			showrivers = False,
			rivercolor = "lightblue",
			showsubunits = True,
			showcountries = True,
			resolution = 50,
			projection = dict(type = "natural earth", scale = 1, rotation = dict(lon = 79, lat = 21))
		),
		title = 'Location Vs Info',
		width = 1000,
		height = 550,
		margin = {'l':5,'r':5,'b':5,'t':40},
		template = 'plotly_dark',
		font = dict(family='Comic Sans MS', size=13)
	)
	plots.update_yaxes(automargin=True)
	return plots

app = dash.Dash(__name__, meta_tags=[{'name': 'viewport','content': 'width=device-width, initial-scale=1.0'}])

# server = app.server			for Heroku Hosting Purpose

app.config['suppress_callback_exceptions'] = True

app.layout = html.Div(style={'height' : '100%', 'margin' : '0px'}, children=[
	html.H1(
		className="h1-1",
		children='Traffic Analysis',
		style={
			'textAlign': 'center',
			'color': 'white'
		}
	),
	html.Div(className = "div-1",children='Real-Time Measurement and Analysis of Internet Traffic', style={
		'textAlign': 'center',
		'color': 'black',
		'fontSize': '20px',
		'fontWeight': '630'
	}),
	html.Div(
	[
		html.Div(
		[
			dcc.RadioItems(
				options=[
					{'label': 'Overview', 'value': 'OR'},
					{'label': 'Real-Time', 'value': 'RT'}
				],
				value='OR',
				labelStyle = {'display' : 'block', 'padding' : '5px'},
				id = 'radio-button-1'
			)
		],
		className = 'div-2',
		style = {'float':'left'}
		),
		html.Div(
		[
			dcc.Tabs(id="tabs"),
			html.Div(id='tabs-content', style = {'padding' : '10px'})
		],
		className = 'div-3',
		style = {'float':'left', 'margin-top': '1%', 'margin-bottom': '1%', 'width': '84%','height': '660px', 'background-color': 'white', 'border-radius': '20px' }
		)
	]
	)
])

@app.callback(Output('tabs', 'children'), [Input('radio-button-1', 'value')])
def set_tab_options(selected_option):
	if selected_option=="OR":
		return [
			dcc.Tab(label='Geographic', value='tab-1'),
			dcc.Tab(label='Plots and Data', value='tab-2'),
		]
		
	elif selected_option=="RT":
		return [
			dcc.Tab(label='Geographic', value='tab-3'),
		]

@app.callback(Output('tabs', 'value'), [Input('radio-button-1', 'value')])
def set_cities_value(selected_option):
	if selected_option=="OR":
		return "tab-1"
	elif selected_option=="RT":
		return "tab-3"

@app.callback(Output('tabs-content', 'children'), [Input('tabs', 'value')])
def render_content(tab):
	if tab == 'tab-1':
		return html.Div(
			[
				html.Div(
					id="graph-2"
				),
				html.Div(
					[
						html.Div(
						[
							dcc.RadioItems(
							options=[
								{'label': 'General', 'value': 'GEN'},
								{'label': 'Traffic Source', 'value': 'TS'},
								{'label': 'Traffic Medium', 'value': 'TM'}
							],
							value='GEN',
							labelStyle = {'display' : 'block', 'padding' : '5px'},
							id = 'radio-button-3'
							)
						],
						style={'float':'left', 'display': 'flex', 'align-items': 'center', 'justify-content': 'center', 'backgroundColor' : '#ffea9e', 'margin-left' : '35px', 'padding' : '5px', 'border-radius' : '15px', 'height' : '100px'}
						),
						html.Div(
							id = 'button-2'
						)
					],
					style = {'display' : 'block', 'align-items': 'center', 'justify-content': 'center'}
				)
			],
			style = {'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
		)
	elif tab == 'tab-2':
		return html.Div(
			[
				html.Div(
					[
						dcc.Graph(
							id='plots-graph-1',
							figure=subplot_overview(),
							style = {'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
						),
					]
				)
			], 
			style = {'height': '70%'}
		)
	elif tab == 'tab-3':
		return html.Div(
			[
				html.Div(
					id="graph-1"
				),
				html.Div(
					[
						html.Div(
						[
							dcc.RadioItems(
							options=[
								{'label': 'Go Live', 'value': 'GL'},
								{'label': 'Update Manually', 'value': 'UM'}
							],
							value='UM',
							labelStyle = {'display' : 'block', 'padding' : '5px'},
							id = 'radio-button-2'
							)
						],
						style={'float':'left', 'display': 'flex', 'align-items': 'center', 'justify-content': 'center', 'backgroundColor' : '#ffea9e', 'margin-left' : '35px', 'padding' : '5px', 'border-radius' : '15px', 'height' : '100px'}
						),
						html.Div(
							id = 'button-1'
						)
					],
					style = {'display' : 'block', 'align-items': 'center', 'justify-content': 'center'}
				)
			],
			style = {'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
			)

def subplot_overview():
	plots = subplots.make_subplots(
		rows=2,cols=3,
		specs= [[{'type':'bar'},{'type':'scatter'},{'type':'pie'}],
				[{'type':'scatter'},{'type':'scatter'},{'type':'table'}]],
		shared_xaxes = True
	)
	fig1,fig2 = plot_bandwidth()
	plots.add_trace(fig1,1,1)
	plots.add_trace(fig2,1,1)
	fig3, fig4, fig5 = plot_system()
	plots.add_trace(fig3,1,2)
	plots.add_trace(fig4,1,2)
	plots.add_trace(fig5,1,2)
	fig6, fig7, fig8 = plot_sessions()
	plots.add_trace(fig6,2,1)
	plots.add_trace(fig7,2,1)
	plots.add_trace(fig8,2,1)
	fig9, fig10, fig11, fig12 = plot_pageviews()
	plots.add_trace(fig9,2,2)
	plots.add_trace(fig10,2,2)
	plots.add_trace(fig11,2,2)
	plots.add_trace(fig12,2,2)
	fig13 = plot_users()
	plots.add_trace(fig13,1,3)
	fig14 = plot_overall()
	plots.add_trace(fig14,2,3)
	plots.update_layout(
		dict(
			template = "plotly_dark",
			width = 1250,
			height = 570,
			margin = {'l':5,'r':5,'b':5,'t':40},
			title = dict(text="Overall Analysis"),
			font = dict(family='Comic Sans MS', size=13)
		)
	)
	return plots

def get_value_bandwidth(date):
	SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
	KEY_FILE_LOCATION = 'YOUR CREDENTIAL FILE LOCATION'
	VIEW_ID = 'YOUR VIEW ID'
	credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
	analytics = build('analyticsreporting', 'v4', credentials=credentials)
	response = analytics.reports().batchGet(
					body={
						'reportRequests': [
						{
							'viewId': VIEW_ID,
							'dateRanges': [{'startDate': date, 'endDate': date}],
							'metrics': [{'expression': 'ga:pageviews'},{'expression': 'ga:users'}],
						}]
					}
				).execute()
	return response

def plot_bandwidth():
	conn = psycopg2.connect(DATABASE_URL)
	cur = conn.cursor()

	today_date = datetime.today().strftime('%Y-%m-%d')
	date_list = pd.date_range(end = datetime.today(), periods = 11).to_pydatetime().tolist()
	date_list.pop()
	for i in range(len(date_list)):
		date_list[i] = date_list[i].strftime('%Y-%m-%d')
	content = []
	query = "select pageviews, users, date, description from bandwidth where date = \'{0}\'"
	for i in date_list:
		cur.execute(query.format(i))
		result = cur.fetchall()
		if len(result) == 0:
			response = get_value_bandwidth(i)
			for report in response.get('reports', []):
				columnHeader = report.get('columnHeader', {})
				dimensionHeaders = columnHeader.get('dimensions', [])
				metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
				if 'rows' not in list(report.get('data', {}).keys()):
					cur.execute("insert into bandwidth(pageviews, users, date, description) values(\'{0}\',\'{1}\',\'{2}\',\'{3}\')".format("0","0",i,""))
					content.append(['0','0',i,''])
				for row in report.get('data', {}).get('rows', []):
					dum = []
					dimensions = row.get('dimensions', [])
					dateRangeValues = row.get('metrics', [])
					for header, dimension in zip(dimensionHeaders, dimensions):
						dum.append(dimension)
					for values in dateRangeValues:
						for value in values.get('values'):
							dum.append(value)
						dum.append(i)
						dum.append('')
					cur.execute("insert into bandwidth(pageviews, users, date, description) values(\'{0}\',\'{1}\',\'{2}\',\'{3}\')".format(dum[0],dum[1],dum[2],dum[3]))
					content.append(dum)
		else:
			content.append(list(result[0]))
	dataf = pd.DataFrame(content, columns=column_names_bandwidth)
	dataf['bandwidth'] = (dataf['users'].astype(int)*dataf['pageviews'].astype(int)*1.55*4.5).map('{:.2f}'.format).astype(float)
	dataf['avgBandwidth'] = (dataf['bandwidth']/dataf['users'].astype(int)).map('{:.2f}'.format).astype(float)
	dataf['text'] = 'Users : '+dataf['users']+'<br>'+'Total Bandwidth per Day : '+dataf['bandwidth'].astype(str)+ " MBps"
	fig1 = go.Bar(x=dataf['date'], y=dataf['bandwidth'], marker = dict(color='indianred'), text = dataf['text'], name="Bandwidth")
	fig2 = go.Bar(x=dataf['date'], y=dataf['avgBandwidth'], marker = dict(color='lightsalmon'), text = 'Avg. Bandwidth per User : '+dataf['avgBandwidth'].astype(str)+" MBps", name="Avg. Bandwidth")
	conn.commit()
	cur.close()
	conn.close()
	return fig1, fig2

def get_value_system1(date):
	SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
	KEY_FILE_LOCATION = 'YOUR CREDENTIAL FILE LOCATION'
	VIEW_ID = 'YOUR VIEW ID'
	credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
	analytics = build('analyticsreporting', 'v4', credentials=credentials)
	response1 = analytics.reports().batchGet(
					body={
						'reportRequests': [
						{
							'viewId': VIEW_ID,
							'dateRanges': [{'startDate': date, 'endDate': date}],
							'dimensions': [{'name': 'ga:operatingSystem'}],
							'metrics' : [{'expression':'ga:users'}]
						}]
					}
				).execute()
	return response1
def get_value_system2(date):
	SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
	KEY_FILE_LOCATION = 'YOUR CREDENTIAL FILE LOCATION'
	VIEW_ID = 'YOUR VIEW ID'
	credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
	analytics = build('analyticsreporting', 'v4', credentials=credentials)
	response2 = analytics.reports().batchGet(
					body={
						'reportRequests': [
						{
							'viewId': VIEW_ID,
							'dateRanges': [{'startDate': date, 'endDate': date}],
							'dimensions': [{'name': 'ga:browser'}],
							'metrics' : [{'expression':'ga:users'}]
						}]
					}
				).execute()
	return response2
def get_value_system3(date):
	SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
	KEY_FILE_LOCATION = 'YOUR CREDENTIAL FILE LOCATION'
	VIEW_ID = 'YOUR VIEW ID'
	credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
	analytics = build('analyticsreporting', 'v4', credentials=credentials)
	response3 = analytics.reports().batchGet(
					body={
						'reportRequests': [
						{
							'viewId': VIEW_ID,
							'dateRanges': [{'startDate': date, 'endDate': date}],
							'dimensions': [{'name': 'ga:deviceCategory'}],
							'metrics' : [{'expression':'ga:users'}]
						}]
					}
				).execute()
	return response3

def plot_system():
	conn = psycopg2.connect(DATABASE_URL)
	cur = conn.cursor()

	today_date = datetime.today().strftime('%Y-%m-%d')
	date_list = pd.date_range(end = datetime.today(), periods = 11).to_pydatetime().tolist()
	date_list.pop()
	for i in range(len(date_list)):
		date_list[i] = date_list[i].strftime('%Y-%m-%d')
	content1, content2, content3 = [], [], []
	query1 = "select operatingSystem, users, date, description from os where date = \'{0}\'"
	query2 = "select browser, users, date, description from browser where date = \'{0}\'"
	query3 = "select deviceCategory, users, date, description from device where date = \'{0}\'"
	for i in date_list:
		cur.execute(query1.format(i))
		result1 = cur.fetchall()
		cur.execute(query2.format(i))
		result2 = cur.fetchall()
		cur.execute(query3.format(i))
		result3 = cur.fetchall()

		if len(result1)==0:
			response1 = get_value_system1(i)
			for report in response1.get('reports', []):
				columnHeader = report.get('columnHeader', {})
				dimensionHeaders = columnHeader.get('dimensions', [])
				metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
				if 'rows' not in list(report.get('data', {}).keys()):
					cur.execute("insert into os(operatingSystem, users, date, description) values({0},\'{1}\',\'{2}\',\'{3}\')".format("NULL","0",i,""))
					content1.append([None,'0',i,''])
				for row in report.get('data', {}).get('rows', []):
					dum = []
					dimensions = row.get('dimensions', [])
					dateRangeValues = row.get('metrics', [])
					for header, dimension in zip(dimensionHeaders, dimensions):
						dum.append(dimension)
					for values in dateRangeValues:
						for value in values.get('values'):
							dum.append(value)
						dum.append(i)
						dum.append('')
					cur.execute("insert into os(operatingSystem, users, date, description) values(\'{0}\',\'{1}\',\'{2}\',\'{3}\')".format(dum[0],dum[1],dum[2],dum[3]))
					content1.append(dum)
		else:
			content1.append(list(result1[0]))

		if len(result2)==0:
			response2 = get_value_system2(i)
			for report in response2.get('reports', []):
				columnHeader = report.get('columnHeader', {})
				dimensionHeaders = columnHeader.get('dimensions', [])
				metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
				if 'rows' not in list(report.get('data', {}).keys()):
					cur.execute("insert into browser(browser, users, date, description) values({0},\'{1}\',\'{2}\',\'{3}\')".format("NULL","0",i,""))
					content2.append([None,'0',i,''])
				for row in report.get('data', {}).get('rows', []):
					dum = []
					dimensions = row.get('dimensions', [])
					dateRangeValues = row.get('metrics', [])
					for header, dimension in zip(dimensionHeaders, dimensions):
						dum.append(dimension)
					for values in dateRangeValues:
						for value in values.get('values'):
							dum.append(value)
						dum.append(i)
						dum.append('')
					cur.execute("insert into browser(browser, users, date, description) values(\'{0}\',\'{1}\',\'{2}\',\'{3}\')".format(dum[0],dum[1],dum[2],dum[3]))
					content2.append(dum)
		else:
			content2.append(list(result2[0]))

		if len(result3)==0:
			response3 = get_value_system3(i)
			for report in response3.get('reports', []):
				columnHeader = report.get('columnHeader', {})
				dimensionHeaders = columnHeader.get('dimensions', [])
				metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
				if 'rows' not in list(report.get('data', {}).keys()):
					cur.execute("insert into device(deviceCategory, users, date, description) values({0},\'{1}\',\'{2}\',\'{3}\')".format("NULL","0",i,""))
					content3.append([None,'0',i,''])
				for row in report.get('data', {}).get('rows', []):
					dum = []
					dimensions = row.get('dimensions', [])
					dateRangeValues = row.get('metrics', [])
					for header, dimension in zip(dimensionHeaders, dimensions):
						dum.append(dimension)
					for values in dateRangeValues:
						for value in values.get('values'):
							dum.append(value)
						dum.append(i)
						dum.append('')
					cur.execute("insert into device(deviceCategory, users, date, description) values(\'{0}\',\'{1}\',\'{2}\',\'{3}\')".format(dum[0],dum[1],dum[2],dum[3]))
					content3.append(dum)
		else:
			content3.append(list(result3[0]))

	conn.commit()
	cur.close()
	conn.close()

	dataf1 = pd.DataFrame(content1, columns=column_names_os)
	dataf2 = pd.DataFrame(content2, columns=column_names_browser)
	dataf3 = pd.DataFrame(content3, columns=column_names_device)
	fig1 = go.Scatter(x=dataf1['date'], y=dataf1['operatingSystem'], marker_size = dataf1['users'].astype(int)*10, mode = "markers", name= "Operating System", text = 'Users : '+dataf1['users'])
	fig2 = go.Scatter(x=dataf2['date'], y=dataf2['browser'],marker_size = dataf2['users'].astype(int)*10, mode = "markers", name = "Browser", text = 'Users : '+dataf2['users'])
	fig3 = go.Scatter(x=dataf3['date'], y=dataf3['deviceCategory'], marker_size = dataf3['users'].astype(int)*10, mode = "markers", name = "Device Category", text = 'Users : '+dataf3['users'])
	return fig1, fig2, fig3

def get_value_sessions(date):
	SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
	KEY_FILE_LOCATION = 'YOUR CREDENTIAL FILE LOCATION'
	VIEW_ID = 'YOUR VIEW ID'
	credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
	analytics = build('analyticsreporting', 'v4', credentials=credentials)
	response = analytics.reports().batchGet(
					body={
						'reportRequests': [
						{
							'viewId': VIEW_ID,
							'dateRanges': [{'startDate': date, 'endDate': date}],
							'metrics' : [{'expression':'ga:sessions'},{'expression':'ga:bounceRate'},{'expression':'ga:hits'}]
						}]
					}
				).execute()
	return response

def plot_sessions():
	conn = psycopg2.connect(DATABASE_URL)
	cur = conn.cursor()

	today_date = datetime.today().strftime('%Y-%m-%d')
	date_list = pd.date_range(end = datetime.today(), periods = 11).to_pydatetime().tolist()
	date_list.pop()
	for i in range(len(date_list)):
		date_list[i] = date_list[i].strftime('%Y-%m-%d')
	content = []
	query = "select sessions, bounceRate, hits, date, description from sessions where date = \'{0}\'"

	for i in date_list:
		cur.execute(query.format(i))
		result = cur.fetchall()
		if len(result)==0:
			response = get_value_sessions(i)
			for report in response.get('reports', []):
				columnHeader = report.get('columnHeader', {})
				dimensionHeaders = columnHeader.get('dimensions', [])
				metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
				if 'rows' not in list(report.get('data', {}).keys()):
					cur.execute("insert into sessions(sessions, bounceRate, hits, date, description) values(\'{0}\',\'{1}\',\'{2}\',\'{3}\',\'{4}\')".format("0","0","0",i,""))
					content.append(['0','0','0',i,''])
				for row in report.get('data', {}).get('rows', []):
					dum = []
					dimensions = row.get('dimensions', [])
					dateRangeValues = row.get('metrics', [])
					for header, dimension in zip(dimensionHeaders, dimensions):
						dum.append(dimension)
					for values in dateRangeValues:
						for value in values.get('values'):
							dum.append(value)
						dum.append(i)
						dum.append('')
					cur.execute("insert into sessions(sessions, bounceRate, hits, date, description) values(\'{0}\',\'{1}\',\'{2}\',\'{3}\',\'{4}\')".format(dum[0],dum[1],dum[2],dum[3],dum[4]))
					content.append(dum)
		else:
			content.append(list(result[0]))
	
	conn.commit()
	cur.close()
	conn.close()

	dataf = pd.DataFrame(content, columns=column_names_sessions)
	dataf['bounceRate'] = (dataf['bounceRate'].astype(float)).map('{:.2f}'.format).astype(float)
	fig1 = go.Scatter(x=dataf['date'], y=dataf['sessions'], marker_size = dataf['sessions'].astype(int)*0.65, mode = "markers+lines", name= "Sessions")
	fig2 = go.Scatter(x=dataf['date'], y=dataf['bounceRate'], marker_size = dataf['bounceRate'].astype(int)*0.65, mode = "markers+lines", name = "Bounce Rate")
	fig3 = go.Scatter(x=dataf['date'], y=dataf['hits'], marker_size = dataf['hits'].astype(int)*0.65, mode = "markers+lines", name = "Hits")
	return fig1, fig2, fig3

def get_value_pageviews(date):
	SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
	KEY_FILE_LOCATION = 'YOUR CREDENTIAL FILE LOCATION'
	VIEW_ID = 'YOUR VIEW ID'
	credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
	analytics = build('analyticsreporting', 'v4', credentials=credentials)
	response = analytics.reports().batchGet(
					body={
						'reportRequests': [
						{
							'viewId': VIEW_ID,
							'dateRanges': [{'startDate': date, 'endDate': date}],
							'metrics' : [{'expression':'ga:pageviews'},{'expression':'ga:pageviewsPerSession'},{'expression':'ga:uniquePageviews'},{'expression':'ga:avgTimeOnPage'}]
						}]
					}
				).execute()
	return response

def plot_pageviews():
	conn = psycopg2.connect(DATABASE_URL)
	cur = conn.cursor()

	today_date = datetime.today().strftime('%Y-%m-%d')
	date_list = pd.date_range(end = datetime.today(), periods = 11).to_pydatetime().tolist()
	date_list.pop()
	for i in range(len(date_list)):
		date_list[i] = date_list[i].strftime('%Y-%m-%d')
	content = []
	query = "select pageviews, pageviewsPerSession, uniquePageviews, avgTimeOnPage, date, description from pageviews where date = \'{0}\'"

	for i in date_list:
		cur.execute(query.format(i))
		result = cur.fetchall()
		if len(result)==0:
			response = get_value_pageviews(i)
			for report in response.get('reports', []):
				columnHeader = report.get('columnHeader', {})
				dimensionHeaders = columnHeader.get('dimensions', [])
				metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
				if 'rows' not in list(report.get('data', {}).keys()):
					cur.execute("insert into pageviews(pageviews, pageviewsPerSession, uniquePageviews, avgTimeOnPage, date, description) values(\'{0}\',\'{1}\',\'{2}\',\'{3}\',\'{4}\',\'{5}\')".format("0","0","0","0",i,""))
					content.append(['0','0','0','0',i,''])
				for row in report.get('data', {}).get('rows', []):
					dum = []
					dimensions = row.get('dimensions', [])
					dateRangeValues = row.get('metrics', [])
					for header, dimension in zip(dimensionHeaders, dimensions):
						dum.append(dimension)
					for values in dateRangeValues:
						for value in values.get('values'):
							dum.append(value)
						dum.append(i)
						dum.append('')
					cur.execute("insert into pageviews(pageviews, pageviewsPerSession, uniquePageviews, avgTimeOnPage, date, description) values(\'{0}\',\'{1}\',\'{2}\',\'{3}\',\'{4}\',\'{5}\')".format(dum[0],dum[1],dum[2],dum[3],dum[4],dum[5]))
					content.append(dum)
		else:
			content.append(list(result[0]))

	conn.commit()
	cur.close()
	conn.close()

	dataf = pd.DataFrame(content, columns=column_names_pageviews)
	dataf['pageviewsPerSession'] = (dataf['pageviewsPerSession'].astype(float)).map('{:.0f}'.format).astype(float)
	dataf['avgTimeOnPage'] = (dataf['avgTimeOnPage'].astype(float)).map('{:.0f}'.format).astype(float)
	dataf['pageviews'] = dataf['pageviews'].astype(float)
	dataf['uniquePageviews'] = dataf['uniquePageviews'].astype(float)

	fig1 = go.Scatter(x=dataf['date'], y=dataf['pageviewsPerSession'], marker=dict(color="ghostwhite", size=12), mode="markers", name = "Pageviews Per Session", opacity=0.7)
	fig2 = go.Scatter(x=dataf['date'], y=dataf['avgTimeOnPage'], marker=dict(color="sandybrown", size=12), mode="markers", name = "Avg. Time On Page", opacity=0.7)
	fig3 = go.Scatter(x=dataf['date'], y=dataf['pageviews'], marker=dict(color="hotpink", size=12), mode="markers", name = "Pageviews", opacity=0.85)
	fig4 = go.Scatter(x=dataf['date'], y=dataf['uniquePageviews'], marker=dict(color="dodgerblue", size=12), mode="markers", name = "Unique Pageviews", opacity=0.7)

	return fig1, fig2, fig3, fig4

def get_value_users():
	SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
	KEY_FILE_LOCATION = 'YOUR CREDENTIAL FILE LOCATION'
	VIEW_ID = 'YOUR VIEW ID'
	credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
	analytics = build('analyticsreporting', 'v4', credentials=credentials)
	response = analytics.reports().batchGet(
					body={
						'reportRequests': [
						{
							'viewId': VIEW_ID,
							'dateRanges': [{'startDate': '2020-05-10', 'endDate': 'today'}],
							'dimensions' : [{'name':'ga:userType'}],
							'metrics' : [{'expression':'ga:users'}]
						}]
					}
				).execute()
	return response

def plot_users():
	content = []
	response = get_value_users()
	for report in response.get('reports', []):
		columnHeader = report.get('columnHeader', {})
		dimensionHeaders = columnHeader.get('dimensions', [])
		metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
		if 'rows' not in list(report.get('data', {}).keys()):
			content.append(['0','0'])
		for row in report.get('data', {}).get('rows', []):
			dum = []
			dimensions = row.get('dimensions', [])
			dateRangeValues = row.get('metrics', [])
			for header, dimension in zip(dimensionHeaders, dimensions):
				dum.append(dimension)
			for values in dateRangeValues:
				for value in values.get('values'):
					dum.append(value)
			content.append(dum)

	dataf = pd.DataFrame(content, columns=column_names_users)
	fig = go.Pie(labels=dataf['visitorType'], values=dataf['users'], hole = 0.3, name ="")
	return fig

def get_value_overall():
	SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
	KEY_FILE_LOCATION = 'YOUR CREDENTIAL FILE LOCATION'
	VIEW_ID = 'YOUR VIEW ID'
	credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
	analytics = build('analyticsreporting', 'v4', credentials=credentials)
	response = analytics.reports().batchGet(
					body={
						'reportRequests': [
						{
							'viewId': VIEW_ID,
							'dateRanges': [{'startDate': '2020-05-10', 'endDate': 'today'}],
							'metrics' : [{'expression':'ga:users'},{'expression':'ga:sessions'},{'expression':'ga:avgSessionDuration'},{'expression':'ga:pageviews'},{'expression':'ga:pageviewsPerSession'},{'expression':'ga:bounceRate'},{'expression':'ga:avgTimeOnPage'},{'expression':'ga:hits'},{'expression':'ga:uniquePageviews'}]
						}]
					}
				).execute()
	return response

def plot_overall():
	content = []
	response = get_value_overall()
	for report in response.get('reports', []):
		columnHeader = report.get('columnHeader', {})
		dimensionHeaders = columnHeader.get('dimensions', [])
		metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
		for row in report.get('data', {}).get('rows', []):
			dum = []
			dimensions = row.get('dimensions', [])
			dateRangeValues = row.get('metrics', [])
			for header, dimension in zip(dimensionHeaders, dimensions):
				dum.append(dimension)
			for values in dateRangeValues:
				for value in values.get('values'):
					dum.append(value)
			content.append(dum)

	content[0][2] = '{:.2f}'.format(float(content[0][2]))
	content[0][4] = '{:.2f}'.format(float(content[0][4]))
	content[0][5] = '{:.2f}'.format(float(content[0][5]))
	content[0][6] = '{:.2f}'.format(float(content[0][6]))

	fig =  go.Table(header = dict(values=['Attributes', 'Value'], fill_color = "#4d004c", font=dict(color='white', size=12.5), height = 21),cells = dict(values=[column_names_overall, content[0]], fill_color = [['#f2e5ff','#ffffff','#f2e5ff','#ffffff','#f2e5ff','#ffffff','#f2e5ff','#ffffff','#f2e5ff']*2], font = dict(size = 12, color = "black"), height = 21))
	return fig

def get_plot_general():
	SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
	KEY_FILE_LOCATION = 'YOUR CREDENTIAL FILE LOCATION'
	VIEW_ID = 'YOUR VIEW ID'
	credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
	analytics = build('analyticsreporting', 'v4', credentials=credentials)
	response = analytics.reports().batchGet(
					body={
						'reportRequests': [
						{
						'viewId': VIEW_ID,
						'dateRanges': [{'startDate': '2020-05-10', 'endDate': 'today'}],
						'dimensions': [{'name': 'ga:country'},{'name': 'ga:region'},{'name': 'ga:city'},{'name': 'ga:longitude'},{'name': 'ga:latitude'}],
						'metrics': [{'expression': 'ga:newUsers'},{'expression': 'ga:sessions'},{'expression': 'ga:UniquePageviews'},{'expression':'ga:bounceRate'},{'expression':'ga:avgSessionDuration'},{'expression': 'ga:hits'}],
						}]
					}
				).execute()
	content = []
	for report in response.get('reports', []):
		columnHeader = report.get('columnHeader', {})
		dimensionHeaders = columnHeader.get('dimensions', [])
		metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
		for row in report.get('data', {}).get('rows', []):
			dum = []
			dimensions = row.get('dimensions', [])
			dateRangeValues = row.get('metrics', [])
			for header, dimension in zip(dimensionHeaders, dimensions):
				dum.append(dimension)
			for values in dateRangeValues:
				for value in values.get('values'):
					dum.append(value)
			dum.append('')
			content.append(dum)
	dataf = pd.DataFrame(content, columns=column_names_overview_geo)
	dataf['text'] = dataf['city']+','+dataf['region']+','+dataf['country']+'<br>'+'Users : '+dataf['users']+'<br>'+'Sessions : '+dataf['sessions']+'<br>'+'Unique Pageviews : '+dataf['UniquePageviews']+'<br>'+'Bounce Rate : '+dataf['bounceRate']+'<br>'+'Avg. Session Duration : '+dataf['avgSessionDuration']+'<br>'+'Hits : '+dataf['hits']
	return get_plot(dataf, 'sessions')

@app.callback(Output('graph-2','children'),[Input('radio-button-3','value')])
def update_general_or_traffic_source(selected_option):
	if selected_option == "GEN":
		return [
			dcc.Graph(
				id='overview-graph-1',
				figure=get_plot_general(),
				style={'float':'left'}
			),
		]
	elif selected_option == "TS":
		return [
			dcc.Graph(
				id='overview-graph-2',
				figure=update_traffic_graph("SRC"),
				style={'float':'left'}
			),
		]
	elif selected_option == "TM":
		return [
			dcc.Graph(
				id='overview-graph-3',
				figure=update_traffic_graph("MDM"),
				style={'float':'left'}
			),
		]

def update_traffic_graph(value):
	SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
	KEY_FILE_LOCATION = 'YOUR CREDENTIAL FILE LOCATION'
	VIEW_ID = 'YOUR VIEW ID'
	credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
	analytics = build('analyticsreporting', 'v4', credentials=credentials)
	if value=="SRC":
		response = analytics.reports().batchGet(
						body={
							'reportRequests': [
							{
							'viewId': VIEW_ID,
							'dateRanges': [{'startDate': '2020-05-10', 'endDate': 'today'}],
							'dimensions': [{'name': 'ga:country'},{'name': 'ga:region'},{'name': 'ga:city'},{'name': 'ga:longitude'},{'name': 'ga:latitude'},{'name': 'ga:source'}],
							'metrics': [{'expression': 'ga:newUsers'}],
							}]
						}
					).execute()
		content = []
		for report in response.get('reports', []):
			columnHeader = report.get('columnHeader', {})
			dimensionHeaders = columnHeader.get('dimensions', [])
			metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
			for row in report.get('data', {}).get('rows', []):
				dum = []
				dimensions = row.get('dimensions', [])
				dateRangeValues = row.get('metrics', [])
				for header, dimension in zip(dimensionHeaders, dimensions):
					dum.append(dimension)
				for values in dateRangeValues:
					for value in values.get('values'):
						dum.append(value)
				dum.append('')
				content.append(dum)
		dataf = pd.DataFrame(content, columns=column_names_source_geo)
		dataf['text'] = dataf['city']+','+dataf['region']+','+dataf['country']+'<br>'+'Users : '+dataf['users']+'<br>'+'Source : '+dataf['source']
		return get_plot(dataf, "latitude")
	elif value=="MDM":
		response = analytics.reports().batchGet(
						body={
							'reportRequests': [
							{
							'viewId': VIEW_ID,
							'dateRanges': [{'startDate': '2020-05-10', 'endDate': 'today'}],
							'dimensions': [{'name': 'ga:country'},{'name': 'ga:region'},{'name': 'ga:city'},{'name': 'ga:longitude'},{'name': 'ga:latitude'},{'name': 'ga:medium'}],
							'metrics': [{'expression': 'ga:newUsers'}],
							}]
						}
					).execute()
		content = []
		for report in response.get('reports', []):
			columnHeader = report.get('columnHeader', {})
			dimensionHeaders = columnHeader.get('dimensions', [])
			metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
			for row in report.get('data', {}).get('rows', []):
				dum = []
				dimensions = row.get('dimensions', [])
				dateRangeValues = row.get('metrics', [])
				for header, dimension in zip(dimensionHeaders, dimensions):
					dum.append(dimension)
				for values in dateRangeValues:
					for value in values.get('values'):
						dum.append(value)
				dum.append('')
				content.append(dum)
		dataf = pd.DataFrame(content, columns=column_names_medium_geo)
		dataf['text'] = dataf['city']+','+dataf['region']+','+dataf['country']+'<br>'+'Users : '+dataf['users']+'<br>'+'Medium : '+dataf['medium']
		return get_plot(dataf, "longitude")

@app.callback([Output('button-1','children'), Output('graph-1','children')],[Input('radio-button-2','value')])
def update_manually_or_go_live(selected_option):
	if selected_option == "GL":
		return [
			[],
			[
				dcc.Graph(
					id='live-graph-1',
					figure=get_plot(df, "users"),
					style={'float':'left'}
				),
				dcc.Interval(
					id='graph-update',
					interval=1000*100
				)
			]
		]
	elif selected_option == "UM":
		return [
			[
				html.Button('Update', id='clicked-button-1', className = "update-button"),
			],
			[
				dcc.Graph(
					id='live-graph-2',
					figure=get_plot(df, "users"),
					style={'float':'left'}
				)
			]
		]

@app.callback(Output('live-graph-1','figure'),[Input('graph-update','n_intervals')])
def update_live_graph(self):
	scope = 'https://www.googleapis.com/auth/analytics.readonly'
	key_file_location = 'YOUR CREDENTIAL FILE LOCATION'
	service = get_service(api_name='analytics', api_version='v3', scopes=[scope], key_file_location=key_file_location)
	profile_id = get_first_profile_id(service)
	results = get_results(service, profile_id)
	rows = results.get('rows',[])
	for row in rows:
		row.append('')
	dataf = pd.DataFrame(rows,columns=column_names_real_time_geo)
	dataf['text'] = dataf['city']+'<br>'+"Users : "+dataf['users']
	return get_plot(dataf, "longitude")

@app.callback(Output('live-graph-2','figure'),[Input('clicked-button-1','n_clicks')])
def update_live_graph(n_clicks):
	scope = 'https://www.googleapis.com/auth/analytics.readonly'
	key_file_location = 'YOUR CREDENTIAL FILE LOCATION'
	service = get_service(api_name='analytics', api_version='v3', scopes=[scope], key_file_location=key_file_location)
	profile_id = get_first_profile_id(service)
	results = get_results(service, profile_id)
	rows = results.get('rows',[])
	for row in rows:
		row.append('')
	dataf = pd.DataFrame(rows,columns=column_names_real_time_geo)
	dataf['text'] = dataf['city']+'<br>'+"Users : "+dataf['users']
	return get_plot(dataf, "latitude")

if __name__ == '__main__':
	app.run_server(debug=True)