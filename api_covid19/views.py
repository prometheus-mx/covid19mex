from django.shortcuts import render
import pandas as pd
import json
import datetime
import sqlite3

ecdc_date = "15 de abril (ajustado)"
ecdc_file = "ecdc_cases_2020.04.15.csv"
confirmed_date = "15 de abril"
confirmed_file = "2020.04.15_confirmed_cases.csv"
suspected_date = "15 de abril"
suspected_file = "2020.04.15_suspected_cases.csv"
file_da = "200415COVID19MEXICO.csv"
dt_da = "15 de abril"

def get_context(dt, file_name):

    df = pd.read_csv("api_covid19/static/files/"+file_name)
    rs = df.groupby("Estado")["Edad"].count().reset_index() \
                      .sort_values('Edad', ascending=False) \
                      .set_index('Estado')
    estados = list(rs.index)
    values = list(rs.values)

    df['RangoDeEdad'] = df.Edad // 10
    rs = df.groupby("RangoDeEdad")["Edad"].count()
    rango_de_edad = list(rs.index)
    v_rango_de_edad = list(rs.values)

    edad_genero = []
    for i, v in enumerate(values):
        values[i] = values[i][0]

    edad_genero.append([0] * len(rango_de_edad))
    edad_genero.append([0] * len(rango_de_edad))
    print(edad_genero)

    rs_edad_genero = df.groupby(["RangoDeEdad", "Sexo"])["N° Caso"].count()
    cur_rango = ''
    i_rango=-1
    print(rs_edad_genero)
    for i, v in enumerate(rs_edad_genero):
        print(rs_edad_genero.index[i])
        if rs_edad_genero.index[i][0] != cur_rango:
            cur_rango=rs_edad_genero.index[i][0]
            i_rango=rango_de_edad.index(cur_rango)
        i_genero = (0 if rs_edad_genero.index[i][1] == "FEMENINO" else 1)
        edad_genero[i_genero][i_rango] = v
    print(edad_genero)

    v_edad_genero = []
    v_edad_genero.append({'name': 'FEMENINO', 'data': edad_genero[0]})
    v_edad_genero.append({'name': 'MASCULINO', 'data': edad_genero[1]})
    print(v_edad_genero)

    for i, v in enumerate(rango_de_edad):
        rango_de_edad[i] = str(rango_de_edad[i] * 10) + '-' + str((rango_de_edad[i] + 1) * 10)

    context = {"estados": estados, 'values': values, 'v_edad_genero': v_edad_genero,
               'rango_de_edad': rango_de_edad, 'v_rango_de_edad' : v_rango_de_edad, 'file_name': file_name, 'dt': dt}
    return context


def confirmed(request):
    context = get_context(confirmed_date, confirmed_file)
    return render(request, 'confirmed.html', context=context)


def suspected(request):
    context = get_context(suspected_date, suspected_file)
    #    table_content = df.to_html(index=None)
    #    table_content = table_content.replace("", "")
    #    table_content = table_content.replace('class="dataframe"', "class='table table-striped'")
    #    table_content = table_content.replace('border="1"', "")
    # 'table_data': table_content
    return render(request, 'suspected.html', context=context)


def index_prev(request):
    df = pd.read_csv("api_covid19/static/files/" + ecdc_file)
    df.dropna(subset=['countryterritoryCode'], inplace=True)
    df = df[df['countryterritoryCode'].str.contains("MEX")]
    df = df[df['cases'] > 0]
    df['dateRep'] = pd.to_datetime(df['dateRep'], format='%d/%m/%Y')
    df['dateRep'] = df['dateRep']  + datetime.timedelta(days=-1)
    fechas = df['dateRep'].tolist()
    for i, v in enumerate(fechas):
        fechas[i] = fechas[i].strftime("%Y/%m/%d")
    fechas.reverse()
    cases = df['cases'].tolist()
    cases.reverse()
    deaths = df['deaths'].tolist()
    deaths.reverse()
    v_fechas = [{'name': 'Confirmados', 'data': cases}, {'name': 'Decesos', 'data': deaths}]
    cases_totals = []
    total = 0
    for i, v in enumerate(cases):
        total += v
        cases_totals.append(total)
    deaths_totals = []
    total = 0
    for i, v in enumerate(deaths):
        total += v
        deaths_totals.append(total)
    v_totals = [{'name': 'Confirmados', 'data': cases_totals}, {'name': 'Decesos', 'data': deaths_totals}]

    df = pd.read_csv("api_covid19/static/files/"+confirmed_file)
    for i, v in enumerate(df.columns):
        df.rename(columns={v: v.replace("\n", "")}, inplace=True)
    df['Fecha de Inicio de síntomas'] = pd.to_datetime(df['Fecha de Inicio de síntomas'], format='%d/%m/%Y')
    rs = df.groupby("Fecha de Inicio de síntomas")["N° Caso"].count()
    fechas_confirmed = list(rs.index)
    v_fechas_confirmed = list(rs.values)
    for i, v in enumerate(fechas_confirmed):
        fechas_confirmed[i] = fechas_confirmed[i].strftime("%Y/%m/%d")
    v_fechas2 = [{'name': 'Casos', 'data': v_fechas_confirmed,
                 'zoneAxis': 'x', 'zones': [{'value': 7}, {'dashStyle': 'dot', 'color': {
    'linearGradient': { 'x1': .25, 'x2': 1, 'y1': 0, 'y2': 0},
    'stops': [
        [0, '#FFDD33'],
        [1, 'white']
    ]
}}]}]
    context = {'fechas': fechas, 'v_fechas': v_fechas, 'fechas2': fechas_confirmed, 'v_fechas2': v_fechas2,
               'v_totals': v_totals,
               'file_name': ecdc_file, 'file_name2': confirmed_file, 'dt': confirmed_date, 'dt_ecdc': ecdc_date}
    return render(request, 'index.html', context=context)

def deaths(request):

    conn = sqlite3.connect("covid19mx.db")
    cur = conn.cursor()
    # cur.execute("SELECT ENTIDAD_UM, count(*) as DEATHS FROM datos_abiertos_MX d " +
    #             "WHERE RESULTADO = 1 AND FECHA_DEF <> '9999-99-99' GROUP BY ENTIDAD_UM ORDER BY count(*) DESC")
    cur.execute("SELECT ENTIDAD_FEDERATIVA, count(*) as DEATHS FROM datos_abiertos_MX d " +
                "JOIN Catalogo_Entidades e ON d.ENTIDAD_UM = e.CLAVE_ENTIDAD " +
                "WHERE RESULTADO = 1 AND FECHA_DEF <> '9999-99-99' GROUP BY ENTIDAD_FEDERATIVA ORDER BY count(*) DESC")
    estados = []
    values = []
    for row in cur:
        estados.append(row[0])
        values.append(row[1])
#    cur.close()

    v_edad_genero = []
    rango_de_edad = []
    por_rango_fem = []
    por_rango_mas = []
    por_rango_sin = []
    v_rango_de_edad = []

    cur.execute("SELECT (EDAD/10) || '0 - ' || (EDAD/10 + 1) || '0' as RANGO_EDAD, c.DESCRIPCIÓN, count(*) as DEATHS "
                "FROM datos_abiertos_MX d JOIN Catalogo_Sexo c ON d.SEXO = c.CLAVE "
                "WHERE RESULTADO = 1 AND FECHA_DEF <> '9999-99-99' "
                "GROUP BY (EDAD/10) || '0 - ' || (EDAD/10 + 1) || '0', SEXO ORDER BY RANGO_EDAD ")
    cur_rango = ''
    for row in cur:
        if cur_rango != row[0]:
            cur_rango = row[0]
            if len(por_rango_fem) < len(rango_de_edad):
                por_rango_fem.append(0)
            if len(por_rango_mas) < len(rango_de_edad):
                por_rango_mas.append(0)
            if len(por_rango_sin) < len(rango_de_edad):
                por_rango_sin.append(0)
            rango_de_edad.append(row[0])
        if row[1] == "MUJER":
            por_rango_fem.append(row[2])
        elif row[1] == "HOMBRE":
            por_rango_mas.append(row[2])
        else:
            por_rango_sin.append(row[2])

    if len(por_rango_fem) < len(rango_de_edad):
        por_rango_fem.append(0)
    if len(por_rango_mas) < len(rango_de_edad):
        por_rango_mas.append(0)
    if len(por_rango_sin) < len(rango_de_edad):
        por_rango_sin.append(0)
    cur.close()
    conn.close()
    print(rango_de_edad)
    print(por_rango_fem)
    print(por_rango_mas)
    print(por_rango_sin)
    for i, v in enumerate(rango_de_edad):
        v_rango_de_edad.append(por_rango_fem[i] + por_rango_mas[i] + por_rango_sin[i])

    v_edad_genero.append({'name': 'FEMENINO', 'data': por_rango_fem})
    v_edad_genero.append({'name': 'MASCULINO', 'data': por_rango_mas})
    if sum(por_rango_sin) > 0:
        v_edad_genero.append({'name': 'NO DEFINIDO', 'data': por_rango_sin})

    context = {"estados": estados, 'values': values, 'v_edad_genero': v_edad_genero,
               'rango_de_edad': rango_de_edad, 'v_rango_de_edad' : v_rango_de_edad, 'file_da': file_da, 'dt': dt_da}
    return render(request, 'deaths.html', context=context)

def index(request):
    df = pd.read_csv("api_covid19/static/files/" + ecdc_file)
    df.dropna(subset=['countryterritoryCode'], inplace=True)
    df = df[df['countryterritoryCode'].str.contains("MEX")]
    df = df[df['cases'] > 0]
    df['dateRep'] = pd.to_datetime(df['dateRep'], format='%d/%m/%Y')
    df['dateRep'] = df['dateRep']  + datetime.timedelta(days=-1)
    fechas = df['dateRep'].tolist()
    fechas.reverse()
    cases = df['cases'].tolist()
    cases.reverse()
    deaths = df['deaths'].tolist()
    deaths.reverse()
    v_fechas = [{'name': 'Confirmados', 'data': cases}, {'name': 'Decesos', 'data': deaths}]
    cases_totals = []
    total = 0
    for i, v in enumerate(cases):
        total += v
        cases_totals.append(total)
    deaths_totals = []
    total = 0
    for i, v in enumerate(deaths):
        total += v
        deaths_totals.append(total)
    v_totals = [{'name': 'Confirmados', 'data': cases_totals}, {'name': 'Decesos', 'data': deaths_totals}]

    conn = sqlite3.connect("covid19mx.db")
    cur = conn.cursor()
    cur.execute("SELECT  (SELECT COUNT(*) FROM datos_abiertos_MX WHERE RESULTADO = 1) as Confirmados" +
        ", (SELECT COUNT(*) FROM datos_abiertos_MX WHERE RESULTADO = 1 AND FECHA_DEF <> '9999-99-99') as Decesos")
    rows = cur.fetchall()
    print(rows)
    print(rows[0][0])
    if cases_totals[len(cases_totals)-1] < rows[0][0]:
        new_cases = rows[0][0] - cases_totals[len(cases_totals)-1]
        new_deaths = rows[0][1] - deaths_totals[len(deaths_totals)-1]
        cases.append(new_cases)
        deaths.append(new_deaths)
        cases_totals.append(rows[0][0])
        deaths_totals.append(rows[0][1])
        fechas.append(fechas[len(fechas)-1] + datetime.timedelta(days=1))
    cur.close()
    conn.close()

    for i, v in enumerate(fechas):
        fechas[i] = fechas[i].strftime("%Y/%m/%d")
    df = pd.read_csv("api_covid19/static/files/"+confirmed_file)
    for i, v in enumerate(df.columns):
        df.rename(columns={v: v.replace("\n", "")}, inplace=True)
    df['Fecha de Inicio de síntomas'] = pd.to_datetime(df['Fecha de Inicio de síntomas'], format='%d/%m/%Y')
    rs = df.groupby("Fecha de Inicio de síntomas")["N° Caso"].count()
    fechas_confirmed = list(rs.index)
    v_fechas_confirmed = list(rs.values)
    for i, v in enumerate(fechas_confirmed):
        fechas_confirmed[i] = fechas_confirmed[i].strftime("%Y/%m/%d")
    v_fechas2 = [{'name': 'Casos', 'data': v_fechas_confirmed,
                 'zoneAxis': 'x', 'zones': [{'value': 7}, {'dashStyle': 'dot', 'color': {
    'linearGradient': { 'x1': .25, 'x2': 1, 'y1': 0, 'y2': 0},
    'stops': [
        [0, '#FFDD33'],
        [1, 'white']
    ]
}}]}]
    context = {'fechas': fechas, 'v_fechas': v_fechas, 'fechas2': fechas_confirmed, 'v_fechas2': v_fechas2,
               'v_totals': v_totals,
               'file_name': ecdc_file, 'file_name2': confirmed_file, 'dt': confirmed_date, 'dt_ecdc': ecdc_date}
    return render(request, 'index.html', context=context)

def last_origin(request):
    dt = "7 de abril de 2020"
    file_name = "2020.04.07_confirmed_cases.csv"
    # read data

    df = pd.read_csv("api_covid19/static/files/"+file_name)
    rs = df.groupby("Estado")["Procedencia"].count().reset_index() \
                      .sort_values('Procedencia', ascending=False) \
                      .set_index('Estado')
    estados = list(rs.index)
    values = list(rs.values)
    rs = df.groupby("Procedencia")["Estado"].count().reset_index() \
                      .sort_values('Estado', ascending=False) \
                      .set_index('Procedencia')
    procedencia = list(rs.index)
    v_procedencia = list(rs.values)
    rs_estado_origen = df.groupby(["Estado", "Procedencia"])["N° Caso"].count()
    df['RangoDeEdad'] = df.Edad // 10
        # str((df["Edad"] % 10)*10) + '-' + str(((df["Edad"] % 10)+1)*10)
    rs = df.groupby("RangoDeEdad")["Procedencia"].count()
    rango_de_edad = list(rs.index)
    v_rango_de_edad = list(rs.values)
    print(rango_de_edad)
    print(v_rango_de_edad)
    proced = []
    for i, v in enumerate(values):
        values[i] = values[i][0]
    for i, v in enumerate(rango_de_edad):
        rango_de_edad[i] = str(rango_de_edad[i] * 10) + '-' + str((rango_de_edad[i] + 1) * 10)
    for i, v in enumerate(v_procedencia):
        v_procedencia[i] = v_procedencia[i][0]
        proced.append([0] * len(estados))
    # for i, v in enumerate(v_rango_de_edad):
    #     v_rango_de_edad[i] = v_rango_de_edad[i][0]
    cur_pais = ''

    print(rango_de_edad)
    print(v_rango_de_edad)
    for i, v in enumerate(rs_estado_origen):
        if rs_estado_origen.index[i][0]!=cur_pais:
            cur_pais=rs_estado_origen.index[i][0]
            i_estado=estados.index(cur_pais)
        proced[procedencia.index(rs_estado_origen.index[i][1])][i_estado] = v

    v_estado_origen = []
    for i, v in enumerate(procedencia):
        v_estado_origen.append({'name': v, 'data': proced[i]})

    if "Estados" in estados:
        i=estados.index("Estados")
        estados[i]="EEUU"
        print(i)
        print(estados[i])

    values_origen = [{'name': 'Casos por origen', 'data': values}]
    context = {"estados": estados, "procedencia": procedencia, "v_procedencia": v_procedencia,
               'values': values, 'values_origen': values_origen, 'v_estado_origen' : v_estado_origen,
               'rango_de_edad': rango_de_edad, 'v_rango_de_edad' : v_rango_de_edad,
               'file_name': file_name, 'dt': dt}
    return render(request, 'last_from.html', context=context)
