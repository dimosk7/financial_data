
## This file contains functions that i wrote to :
       # 1) create a MySQL table with same columns as dataframe's ---> create_table()
       # 2) store the data of a pandas dataframe to a MySQL table --> insert_to_sql()

import pandas as pd
import numpy as np
import mysql.connector
from datetime import datetime
import math


def create_table(dataframe, table, database, time = False) :
    dataframe = dataframe.copy(deep = True)
    for i in dataframe.loc[:, dataframe.dtypes == object].columns:
        dataframe.loc[:, i] = dataframe.loc[:, i].apply(lambda x: str(x) if not pd.isna(x) else "")
    names = []
    cnt = 0
    for j in dataframe.dtypes:
        if j == np.float64:
            names.append("`{}`".format(dataframe.dtypes.index[cnt]) + "float")
        elif j == np.int64:
            max_int = dataframe[dataframe.dtypes.index[cnt]].max()
            if max_int >= 2147483647 :
                names.append("`{}`".format(dataframe.dtypes.index[cnt]) + "bigint")
            else :
                names.append("`{}`".format(dataframe.dtypes.index[cnt]) + "int")
        elif j == object:
            max_len = dataframe[dataframe.dtypes.index[cnt]].apply(len).max()
            if max_len < 255 :
                names.append("`{}`".format(dataframe.dtypes.index[cnt]) + "char({})".format(max_len))
            else :
                names.append("`{}`".format(dataframe.dtypes.index[cnt]) + "text({})".format(max_len))
        elif j == "datetime64[ns]":
            if time :
                names.append("`{}`".format(dataframe.dtypes.index[cnt]) + "datetime")
            else :
                names.append("`{}`".format(dataframe.dtypes.index[cnt]) + "date")
        cnt += 1

    names = ",".join(names)

    query = "CREATE TABLE IF NOT EXISTS {}({})".format(table, names)
    mycursor = database.cursor()
    mycursor.execute(query)
    database.commit()


def insert_to_sql(dataframe, table, database) :
    dataframe = dataframe.copy(deep=True)
    for x in dataframe.loc[:, dataframe.dtypes == "datetime64[ns]"].columns:
        dataframe.loc[:, x] = dataframe.loc[:, x].apply(str)

    st = "("
    for i in range(len(dataframe.columns)):
      if i == len(dataframe.columns) - 1:
        st = st + "%s"
      else:
        st = st + "%s,"
    st = st + ")"

    mycursor = database.cursor()
    for x in range(len(dataframe)) :
      sql = "INSERT INTO {} VALUES {}".format(table, st)
      t = []
      for j in tuple(dataframe.iloc[x, :]) :
        if isinstance(j, (int, float, np.int64, np.float64)) and np.isnan(j) :
            t.append(None)
        elif isinstance(j, (float, np.float64)) :
            t.append(float(j))
        elif isinstance(j, (int, np.int64)) :
            t.append(int(j))
        elif isinstance(j, object) and not j:
            t.append(None)
        elif isinstance(j, object) :
            t.append(j)

      val = tuple(t)
      mycursor.execute(sql, val)
      database.commit()







