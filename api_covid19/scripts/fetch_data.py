import urllib
from os.path import basename

import PyPDF2
import pandas
import requests
import camelot.io as camelot
import pandas as pd
import glob
import json
import os
import re
import warnings
from datetime import datetime
from bs4 import BeautifulSoup
import zipfile
try:
    import zlib
    compression = zipfile.ZIP_DEFLATED
except:
    compression = zipfile.ZIP_STORED

modes = { zipfile.ZIP_DEFLATED: 'deflated',
          zipfile.ZIP_STORED:   'stored',
          }

def downloadFile(url, filename, location='files/'):
    """
    Download file from given {url} and store file in disk
    Arguments:
        url -- File to download
        filename -- Name of the file to store in disk without .pdf extension
    """
#    url = url
    r = requests.get(url, allow_redirects=False, stream=True) #,
    success = r.status_code == requests.codes.ok

    if success:
        with open(f'api_covid19/{location}{filename}', 'wb') as f:
            f.write(r.content)
    else:
        warnings.warn(f"********", FutureWarning)
        warnings.warn(url + ' file not found', FutureWarning)

    return success

def getPagesNumber(filename):
    """
    Read PDF file and return number of pages
    Arguments:
        filename -- PDF to read without .pdf extension
    Return:
        Number of PDF pages
    """
    file = open(f'api_covid19/files/{filename}.pdf', 'rb')
    file_reader = PyPDF2.PdfFileReader(file)

    return file_reader.numPages;

def generateCSV(filename):
    """
    Read PDF files and then create a CSV equivalent.
    Arguments:
        filename -- PDF to read without .pdf extension
    """
    print(".", end='')
    # Get path and number of pages
    n_pages = getPagesNumber(filename)
    file_path = f'api_covid19/files/{filename}.pdf'

    # Convert PDF to CSV
    print(".", end='')
    tables = camelot.read_pdf(file_path, pages=f'1-{n_pages}', split_text=True)
    print(".", end='')
    tables.export(f'api_covid19/files/intermediate_{filename}.csv', f='csv', compress=False)
    print(".", end='')

    # Merge generated CSV files into just one
    all_filenames = [i for i in sorted(glob.glob(f'api_covid19/files/intermediate_{filename}*.csv'))]
    combined_csv = pd.read_csv(all_filenames[0])
    print(".", end='')

    for idx, f in enumerate(all_filenames):
        if idx > 0:
            df = pd.read_csv(f, header=None)
            df.columns = combined_csv.columns
            combined_csv = combined_csv.append(df)

    print(".", end='')
    combined_csv.to_csv(f'api_covid19/static/files/{filename}.csv', index=False, encoding='utf-8-sig')

    # Finally remove intermediate CSV files
    for f in all_filenames:
        os.remove(f)


def getPDFLinks():
    """
    Scrap https://www.gob.mx/ website to get last links of the thecnical documents.
    Return:
        Dictionary with PDF links of confirmed and positives cases
    """
    url = 'https://www.gob.mx/salud/documentos/coronavirus-covid-19-comunicado-tecnico-diario-238449'
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    links = {}

    html_table = soup.find('div', class_='table-responsive')

    for a in html_table.find_all('a'):
        if 'positivos' in a['href']:
            links['confirmed_cases'] = 'https://www.gob.mx/' + a['href']
        elif 'sospechosos' in a['href']:
            links['suspected_cases'] = 'https://www.gob.mx/' + a['href']

    return links


def proc_download(url, filename, location='files/'):
    if os.path.exists(f'api_covid19/{location}{filename}'):
        print(filename + " ya existía")
    else:
        downloadFile(url=url, filename=filename, location=location)
        print(filename + " descargado")


def run_prev():
    print("Iniciando")
    pdf_links = getPDFLinks()
     # Extract date from document URL
    report_date = re.findall('[0-9]*\.[0-9]*\.[0-9]*', pdf_links['confirmed_cases'])[0]

    # Documents filenames
    cc_filename = f'{report_date}_confirmed_cases' # Confirmed cases filename
    sc_filename = f'{report_date}_suspected_cases' # Suspected cases filename
    print(pdf_links)

    proc_download(pdf_links['confirmed_cases'], cc_filename+'.pdf')
    proc_download(pdf_links['suspected_cases'], sc_filename+'.pdf')

    if os.path.exists(f'api_covid19/static/files/{cc_filename}.csv'):
        print(cc_filename + ".csv ya existía")
    else:
        generateCSV(cc_filename)
        print(cc_filename + ".csv generado")

    # if os.path.exists(f'api_covid19/static/files/{sc_filename}.csv'):
    #     print(sc_filename + ".csv ya existía")
    # else:
    #     generateCSV(sc_filename)
    #     print(sc_filename + ".csv generado")

def cleandb(conn):
    cursor = conn.cursor()
    sql_update_query = """DELETE FROM datos_abiertos_MX WHERE RESULTADO <> 1 AND (NEUMONIA <> 1 OR FECHA_DEF = '9999-99-99')"""
    cursor.execute(sql_update_query)
    conn.commit()
    conn.execute("VACUUM")

def run():
    print("Iniciando")

    to_path = 'static/files/'
    ecdc_url = f"https://opendata.ecdc.europa.eu/covid19/casedistribution/csv"
    ecdc_filename = "ecdc_cases_" + datetime.today().strftime("%Y.%m.%d") + '.csv'
    # proc_download(ecdc_url, ecdc_filename, to_path)
    # u2 = urllib.request.urlopen(ecdc_url)
    # for lines in u2.readlines():
    #     print(lines)
    if os.path.exists(f'api_covid19/{to_path}{ecdc_filename}'):
        print(f'{ecdc_filename} ya existía')
    else:
        df = pd.read_csv(ecdc_url)
        df.to_csv('api_covid19/' + to_path + ecdc_filename)
        print("Archivo " + ecdc_filename + " guardado.")

    datos_abiertos = 'api_covid19/' + to_path + datetime.today().strftime("%y%m%d") + 'COVID19MEXICO.csv'
    if os.path.exists(datos_abiertos):
        print(f'{datos_abiertos} ya existía')

        df = pandas.read_csv(datos_abiertos, encoding = "latin")

        import sqlite3
        conn = sqlite3.connect("covid19mx.db")
        df.to_sql("datos_abiertos_MX", conn, if_exists='replace', index='id')
        print("Datos Abiertos copiados a SQLLITE")
        cleandb(conn)
        conn.close()

        zf = zipfile.ZipFile(datos_abiertos.replace('.csv', '.zip'), mode='w')
        try:
            print('adding ' + datos_abiertos + ' to ZIP -- ', modes[compression])
            zf.write(datos_abiertos, basename(datos_abiertos), compress_type=compression)
        finally:
            zf.close()

        os.remove(datos_abiertos)
#         from sqlalchemy import create_engine
#         import pyodbc
#         import urllib
# #{ODBC Driver 13 for SQL Server}
#         params = urllib.parse.quote_plus(r'Driver={ODBC Driver 13 for SQL Server};Server=tcp:csoriano.database.windows.net,1433;Database=covid19mx;Uid=csoriano;Pwd={d8c0b1d+};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
#         conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
#         engine = create_engine(conn_str, echo=True)
#
#         #engine = create_engine('mssql+pyodbc://csoriano:d8c0b1d+@csoriano.database.windows.net:1433/covid19mx?driver=SQL+Server+Native+Client+10.0')
#         #engine = create_engine("mssql+pyodbc://csoriano:d8c0b1d@csoriano.database.windows.net:1433/covid19mx?driver=ODBC+Driver+13+for+SQL+Server")
#         df.to_sql("datos_abiertos_MX", engine, if_exists='replace', index='id')
#         engine.close()


    else:
        da_url = f"http://187.191.75.115/gobmx/salud/datos_abiertos/datos_abiertos_covid19.zip"
        da_filename = "tmp_datos_abiertos_covid19.zip"
        proc_download(da_url, da_filename, to_path)

        with zipfile.ZipFile('api_covid19/' + to_path + da_filename, 'r') as zip_ref:
            da_file = zip_ref.namelist()[0];
            zip_ref.extractall('api_covid19/' + to_path)

        datos_abiertos = 'api_covid19/' + to_path + da_file
        print("Datos Abiertos File = " + datos_abiertos)
        df = pandas.read_csv(datos_abiertos, encoding = "latin")

        import sqlite3
        conn = sqlite3.connect("covid19mx.db")
        df.to_sql("datos_abiertos_MX", conn, if_exists='replace', index='id')

        cleandb(conn)
        conn.close()

        os.rename('api_covid19/' + to_path + da_filename, datos_abiertos.replace('.csv', '.zip'))
        os.remove(datos_abiertos)

        from sqlalchemy import create_engine
        import pyodbc

        # db = {'servername': 'tcp:csoriano.database.windows.net,1433',
        #       'database': 'covid19mx',
        #       'driver': 'driver=SQL Server Native Client 11.0'}

 #       engine = create_engine('mssql+pyodbc://csoriano:d8c0b1d+@csoriano.database.windows.net:1433/covid19mx?driver=SQL+Server+Native+Client+10.0')
        # create the connection
        # engine = create_engine('mssql+pyodbc://' + db['servername'] + '/' + db['database'] + "?" + db['driver'])

 #       df.to_sql("datos_abiertos_MX", engine, if_exists='replace', index='id')
 #       engine.close()

        print("Datos Abiertos copiados a la BD")

#    run_prev()

if __name__ == '__main__':
    run()