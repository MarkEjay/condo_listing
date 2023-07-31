import requests
from bs4 import BeautifulSoup
import pandas as pd
from re import sub
from decimal import Decimal
import time
import plotly.express as px
from dash import Dash, html, dcc


app = Dash(__name__)



HEADERS=()
apiKey=""


url =f'https://condos.ca/toronto?sublocality_id=14&mode=Rent&beds=1-1&bathrooms=1&property_type=Condo%20Apt,Comm%20Element%20Condo,Leasehold%20Condo&map_bounds=-79.41094641084048,43.631012253246354,-79.35863296991072,43.67718863883189'


page=requests.get(url,headers=HEADERS)
soup = BeautifulSoup(page.text, 'lxml')
pages=soup.find('ul', class_='styles___Pagination-sc-a7byxw-0 kqZJpr')


page_length=(len(pages.text))
address_list = []
details_list=[]
price_list=[]


for x in range(1,page_length+1):
    url =f'https://condos.ca/toronto?sublocality_id=14&mode=Rent&beds=1-1&bathrooms=1&property_type=Condo%20Apt%2CComm%20Element%20Condo%2CLeasehold%20Condo&tab=listings&page='
    url +=f"{x}"
    page=requests.get(url,headers=HEADERS)
    soup = BeautifulSoup(page.text, 'lxml')

    address = soup.find_all('address', class_='styles___Address-sc-54qk44-13 kJLXXT')
    for i in address:
        _address = i.text+', Toronto'
        # print(_address)
        address_list.append(_address)

    details = soup.find_all('div', class_='styles___InfoHolder-sc-54qk44-7 buduQR')
    for i in details:
        _details = i.text
        details_list.append(_details)

    price=soup.find_all('div',class_='styles___AskingPrice-sc-54qk44-4 deOfjO')
    for i in price:
        _price = i.text
        value = Decimal(sub(r'[^\d.]', '', _price))

        # print(value)
        price_list.append(_price)



# With Batch Geocoding, you create a geocoding job by sending addresses and then, after some time, get geocoding results by job id
# You may require a few attempts to get results. Here is a timeout between the attempts - 1 sec. Increase the timeout for larger jobs.
timeout = 10

# Limit the number of attempts
maxAttempt = 100
lat_list=[]
lon_list=[]

def getLocations(locations):
    size=len(locations)
    url = "https://api.geoapify.com/v1/batch/geocode/search?apiKey=" + apiKey
    response = requests.post(url, json = locations)
    result = response.json()

    # The API returns the status code 202 to indicate that the job was accepted and pending
    status = response.status_code
    if (status != 202):
        print('Failed to create a job. Check if the input data is correct.')
        return
    jobId = result['id']
    getResultsUrl = url + '&id=' + jobId

    time.sleep(timeout)
    result = getLocationJobs(getResultsUrl, 0,size)
    if (result):
        print(result)
        print('You can also get results by the URL - ' + getResultsUrl)
    else:
        print('You exceeded the maximal number of attempts. Try to get results later. You can do this in a browser by the URL - ' + getResultsUrl)

def getLocationJobs(url, attemptCount,size):
    response = requests.get(url)
    result = response.json()
    status = response.status_code
    if (status == 200):
        print('The job is succeeded. Here are the results:')
        for x in range(size):
            lon_list.append(result[x]['lon'])
            lat_list.append(result[x]['lat'])
    elif (attemptCount >= maxAttempt):
        return
    elif (status == 202):
        print('The job is pending...')
        time.sleep(timeout)
        return getLocationJobs(url, attemptCount + 1,size)


getLocations(address_list)

table = pd.DataFrame({
    'Address':address_list,
    'Details':details_list,
    'Price':price_list,
    'Lat':lat_list,
    'Lon':lon_list
    })

table.index.name = "ID"
table.to_csv("toronto_condo_rent(1BD).csv")

toronto_condo = pd.read_csv('toronto_condo_rent(1BD).csv')

fig = px.scatter_mapbox(toronto_condo, lat="Lat", lon="Lon",hover_data=["Price"], hover_name="Address",
                        color_discrete_sequence=["fuchsia"], zoom=3, height=500)
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
# fig.show()

app.layout = html.Div(children=[
    html.H1(children='Hello Toronto'),

    html.Div(children='''
        A map view of 1bedroom condos in toronto.
    '''),

    dcc.Graph(
        id='example-graph',
        figure=fig
    )
])

if __name__ == '__main__':
    app.run(debug=True, port=8052)
