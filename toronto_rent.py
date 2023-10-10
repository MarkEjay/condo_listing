import boto3
import s3fs
import redshift_connector 
import requests
from bs4 import BeautifulSoup
import pandas as pd
from re import sub
from decimal import Decimal
import time
import plotly.express as px
from dash import Dash, html, dcc
from sqlalchemy import text


AWS_ACCESS_KEY=''
AWS_SECRET_ACCESS_KEY=''
AWS_USERNAME=''
AWS_PASSWORD=''
AWS_PORT=''
AWS_HOST=''

app = Dash(__name__)
def testing():
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )


    foo = pd.DataFrame({'x':[1,2,3], 'y':['a','b','c']})

    # foo.to_csv('s3://condo-scape/foo.csv')
    foo.index.name="ID"
    bytes_to_write = foo.to_csv(None).encode()
    fs = s3fs.S3FileSystem(key=AWS_ACCESS_KEY, secret=AWS_SECRET_ACCESS_KEY)
    with fs.open('s3://condo-scape/foo.csv', 'wb') as f:
        f.write(bytes_to_write)

    # s3.Bucket('condo-scape').upload_file(Filename='foo.csv',Key='foo.csv')

    # https://condo-scape.s3.amazonaws.com/foo.csv


    # for obj in s3.Bucket('condo-scape').objects.all():
    #     print(obj)

    obj = s3.Bucket('condo-scape').Object('foo.csv').get()
    # soo= pd.read_csv(obj['Body'], index_col=0)
def extract_rental():
    HEADERS = ({'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36', 'Accept-Language':'en-US, en;q=0.5'})
    geoKey = ""
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    # HEADERS=()
    # apiKey=""


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
            #convert currency to floating number
            value = Decimal(sub(r'[^\d.]', '', _price))

            # print(value)
            price_list.append(value)



    # With Batch Geocoding, you create a geocoding job by sending addresses and then, after some time, get geocoding results by job id
    # You may require a few attempts to get results. Here is a timeout between the attempts - 1 sec. Increase the timeout for larger jobs.
    timeout = 10

    # Limit the number of attempts
    maxAttempt = 200
    lat_list=[]
    lon_list=[]

    def getLocations(locations):
        size=len(locations)
        url = "https://api.geoapify.com/v1/batch/geocode/search?apiKey=" + geoKey
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
        
        })
    cord = pd.DataFrame({
        'Lat':lat_list,
        'Lon':lon_list
    })

    result = table.join(cord)

    result.index.name = "ID"

    # table = pd.DataFrame({
    #     'Address':address_list,
    #     'Details':details_list,
    #     'Price':price_list,
    #     'Lat':lat_list,
    #     'Lon':lon_list
    #     })
    # table.index.name = "ID"

    result.to_csv("toronto_condo_rent(1BD).csv")
    # toronto_condo = pd.read_csv('toronto_condo_rent(1BD).csv')

    bytes_to_write = result.to_csv(None).encode()
    fs = s3fs.S3FileSystem(key=AWS_ACCESS_KEY, secret=AWS_SECRET_ACCESS_KEY)
    with fs.open('s3://condo-scape/toronto_condo_rent(1BD).csv', 'wb') as f:
        f.write(bytes_to_write)

    obj = s3.Bucket('condo-scape').Object('toronto_condo_rent(1BD).csv').get()



def load_to_redshift():
    conn = redshift_connector.connect(
        host=AWS_HOST,
        database='dev',
        port=AWS_PORT,
        user=AWS_USERNAME,
        password=AWS_PASSWORD
    )

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE Rental (
        ID int NOT NULL,
        Address varchar(255),
        Details varchar(255) ,
        Price int ,
        Lat DOUBLE PRECISION ,
        Lon DOUBLE PRECISION ,

        PRIMARY KEY (ID)
    );

    """)
    conn.commit()


    cursor.execute("""
    COPY dev.public.rental FROM 's3://condo-scape/toronto_condo_rent(1BD).csv' IAM_ROLE 'arn:aws:iam::416952013722:role/service-role/AmazonRedshift-CommandsAccessRole-20231003T162544' FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'us-east-1'
    """)
    conn.commit()
    conn.close()
    # cursor.execute("select * from rental")
    # print(cursor.fetchall())
    # conn.commit()
    
    

# testing()
# extract_rental()
load_to_redshift()

# toronto_condo = pd.read_csv(obj['Body'], index_col=0)

# fig = px.scatter_mapbox(toronto_condo, lat="Lat", lon="Lon",hover_data=["Price"], hover_name="Address",
#                         color_discrete_sequence=["fuchsia"], zoom=3, height=500)
# fig.update_layout(mapbox_style="open-street-map")
# fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
# # fig.show()

# app.layout = html.Div(children=[
#     html.H1(children='Hello Toronto'),

#     html.Div(children='''
#         A map view of 1bedroom condos in toronto.
#     '''),

#     dcc.Graph(
#         id='example-graph',
#         figure=fig
#     )
# ])

# if __name__ == '__main__':
#     app.run(debug=True, port=8052)

