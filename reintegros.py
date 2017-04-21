#coding: utf-8
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from operator import add
from sys import argv,exit
import numpy as np
from pyspark.sql.types import Row
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType

def mov_atipico(valores):  #recibe un array de datos
    valores = [x for x in valores if str(x) != 'nan' and str(x) != 'inf' and x > 0]
    N = len(valores)
    if N > 5 and N < 40000:  #aplicarle alguna condicion mas	
        k = 3
        li_it = np.mean(valores) - k*((np.var(valores))**(0.5))
        ls_it = np.mean(valores) + k*((np.var(valores))**(0.5))
        #se construye un test de outliers mediante el rango intercuartilico
        Q1 = np.median(sorted(valores)[:(len(valores)/2+1)])  #primer cuartil
        Q3 = np.median(sorted(valores)[len(valores)/2:])  #tercer cuartil
        RI = int(Q3-Q1) #rango intercuartilico
        coef = 3  #se puede variar
        li_ri = Q1-RI*coef
        ls_ri = Q3+RI*coef
        if li_ri == ls_ri:
            li_ri = np.mean([li_ri, min(valores)])
            ls_ri = np.mean([ls_ri, max(valores)])
        val_fuera = 0
        li = min(li_it,li_ri) 
        ls = max(ls_it,ls_ri)
        for j in valores:
            if j < li or j > ls:
                val_fuera += 1
        contador = 0
        if float(val_fuera)/N > 0.02: #compruebo si queda fuera mas de un 2% de los datos que tenemos
            ls = np.mean([ls, max(valores)])
            li = np.mean([li, min(valores)])
            for i in range(len(valores)):
                if valores[i] > ls or valores[i] < li:
                    contador += 1
            if float(contador)/N > 0.02:
                ls = max(valores)
                li = min(valores)
        if round(li) == round(ls):
            li = 0
            ls = ls + 0.3*ls
        intervalo = [max(li,0), ls]
    elif N >= 40000:
        li = min(valores)
        ls = max(valores) 
        intervalo = [li, ls]
    elif N <= 5:
        li = -np.inf
        ls = np.inf
        intervalo = [li, ls]
    return intervalo
# PARAMS: 
# appName = argv[1] 
# clientes_fcst = argv[2] 

# Spark context 
sc = SparkContext(appName = "Modelo1")
# Hive Context 
hc = HiveContext(sc)
df_cajeros=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="288" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_cajeros.map(lambda x: (x[0],x[1])).groupByKey().mapValues(list)
rdd_cajeros_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_cajeros_intervalos.map(lambda x: Row(str(x[0]), str(x[1][0]), str(x[1][1])))
schema = StructType([StructField("titular", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.eventos_rt_mov_reintegros")
hc.sql("create table datalabin.eventos_rt_mov_reintegros as select * from tabla_temporal ")
