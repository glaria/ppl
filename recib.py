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
    if N > 5 and N < 30000:  #aplicarle alguna condicion mas	
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
        if float(val_fuera)/N > 0.01: #compruebo si queda fuera mas de un 1% de los datos que tengo
            ls = np.mean([ls, max(valores)])
            li = np.mean([li, min(valores)])
            for i in range(len(valores)):
                if valores[i] > ls or valores[i] < li:
                    contador += 1
            if float(contador)/N > 0.01:
                ls = max(valores)
                li = min(valores)
        if li == ls:
            li = -np.inf
            ls = np.inf
        intervalo = [0, ls]
    elif N >= 30000:
        li = 0
        ls = max(valores)
        intervalo = [li, ls]
    elif N <= 5:
        li = -np.inf
        ls = np.inf
        intervalo = [li, ls]
    return intervalo

# Spark context 
sc = SparkContext(appName = "Modelo3")
# Hive Context 
hc = HiveContext(sc)

df_recibos=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.co1504 as string) as co1504 , cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="254" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_recibos.map(lambda x: ((x[0],x[1]),x[2])).groupByKey().mapValues(list)
rdd_recibos_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_recibos_intervalos.map(lambda x: Row(str(x[0][0]),str(x[0][1]), str(x[1][0]), str(max(x[1][1],14.9)))) #edicion del 19/04 para el limite superior se elige el maximo entre el percentil 1 (calculado previamente) y el valor del modelo
schema = StructType([StructField("titular", StringType(), True),StructField("co1504", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.mov_atipicos_Ragua")
hc.sql("create table datalabin.mov_atipicos_Ragua as select * from tabla_temporal ")

df_recibos=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.co1504 as string) as co1504 , cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="265" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_recibos.map(lambda x: ((x[0],x[1]),x[2])).groupByKey().mapValues(list)
rdd_recibos_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_recibos_intervalos.map(lambda x: Row(str(x[0][0]),str(x[0][1]), str(x[1][0]), str(max(x[1][1],41.7))))
schema = StructType([StructField("titular", StringType(), True),StructField("co1504", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.mov_atipicos_Ralmacenes")
hc.sql("create table datalabin.mov_atipicos_Ralmacenes as select * from tabla_temporal ")

df_recibos=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.co1504 as string) as co1504 , cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="260" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_recibos.map(lambda x: ((x[0],x[1]),x[2])).groupByKey().mapValues(list)
rdd_recibos_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_recibos_intervalos.map(lambda x: Row(str(x[0][0]),str(x[0][1]), str(x[1][0]), str(max(x[1][1],13.2))))
schema = StructType([StructField("titular", StringType(), True),StructField("co1504", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.mov_atipicos_Ralquileres")
hc.sql("create table datalabin.mov_atipicos_Ralquileres as select * from tabla_temporal ")

df_recibos=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.co1504 as string) as co1504 , cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="263" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_recibos.map(lambda x: ((x[0],x[1]),x[2])).groupByKey().mapValues(list)
rdd_recibos_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_recibos_intervalos.map(lambda x: Row(str(x[0][0]),str(x[0][1]), str(x[1][0]), str(max(x[1][1],6.2))))
schema = StructType([StructField("titular", StringType(), True),StructField("co1504", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.mov_atipicos_Rbeneficas")
hc.sql("create table datalabin.mov_atipicos_Rbeneficas as select * from tabla_temporal ")

df_recibos=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.co1504 as string) as co1504 , cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="256" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_recibos.map(lambda x: ((x[0],x[1]),x[2])).groupByKey().mapValues(list)
rdd_recibos_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_recibos_intervalos.map(lambda x: Row(str(x[0][0]),str(x[0][1]), str(x[1][0]), str(max(x[1][1],24.1))))
schema = StructType([StructField("titular", StringType(), True),StructField("co1504", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.mov_atipicos_Rcolegio")
hc.sql("create table datalabin.mov_atipicos_Rcolegio as select * from tabla_temporal ")

df_recibos=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.co1504 as string) as co1504 , cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="266" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_recibos.map(lambda x: ((x[0],x[1]),x[2])).groupByKey().mapValues(list)
rdd_recibos_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_recibos_intervalos.map(lambda x: Row(str(x[0][0]),str(x[0][1]), str(x[1][0]), str(max(x[1][1],30.7))))
schema = StructType([StructField("titular", StringType(), True),StructField("co1504", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.mov_atipicos_Rcomunidades")
hc.sql("create table datalabin.mov_atipicos_Rcomunidades as select * from tabla_temporal ")

df_recibos=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.co1504 as string) as co1504 , cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="253" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_recibos.map(lambda x: ((x[0],x[1]),x[2])).groupByKey().mapValues(list)
rdd_recibos_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_recibos_intervalos.map(lambda x: Row(str(x[0][0]),str(x[0][1]), str(x[1][0]), str(max(x[1][1],11.4))))
schema = StructType([StructField("titular", StringType(), True),StructField("co1504", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.mov_atipicos_Rgas")
hc.sql("create table datalabin.mov_atipicos_Rgas as select * from tabla_temporal ")

df_recibos=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.co1504 as string) as co1504 , cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="250" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_recibos.map(lambda x: ((x[0],x[1]),x[2])).groupByKey().mapValues(list)
rdd_recibos_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_recibos_intervalos.map(lambda x: Row(str(x[0][0]),str(x[0][1]), str(x[1][0]), str(max(x[1][1],45.8))))
schema = StructType([StructField("titular", StringType(), True),StructField("co1504", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.mov_atipicos_Rletra")
hc.sql("create table datalabin.mov_atipicos_Rletra as select * from tabla_temporal ")

df_recibos=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.co1504 as string) as co1504 , cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="264" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_recibos.map(lambda x: ((x[0],x[1]),x[2])).groupByKey().mapValues(list)
rdd_recibos_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_recibos_intervalos.map(lambda x: Row(str(x[0][0]),str(x[0][1]), str(x[1][0]), str(max(x[1][1],10.4))))
schema = StructType([StructField("titular", StringType(), True),StructField("co1504", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.mov_atipicos_Rlibrerias")
hc.sql("create table datalabin.mov_atipicos_Rlibrerias as select * from tabla_temporal ")

df_recibos=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.co1504 as string) as co1504 , cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="251" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_recibos.map(lambda x: ((x[0],x[1]),x[2])).groupByKey().mapValues(list)
rdd_recibos_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_recibos_intervalos.map(lambda x: Row(str(x[0][0]),str(x[0][1]), str(x[1][0]), str(max(x[1][1],16.5))))
schema = StructType([StructField("titular", StringType(), True),StructField("co1504", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.mov_atipicos_Rluz")
hc.sql("create table datalabin.mov_atipicos_Rluz as select * from tabla_temporal ")

df_recibos=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.co1504 as string) as co1504 , cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="255" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_recibos.map(lambda x: ((x[0],x[1]),x[2])).groupByKey().mapValues(list)
rdd_recibos_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_recibos_intervalos.map(lambda x: Row(str(x[0][0]),str(x[0][1]), str(x[1][0]), str(max(x[1][1],72.8))))
schema = StructType([StructField("titular", StringType(), True),StructField("co1504", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.mov_atipicos_Rprestamo")
hc.sql("create table datalabin.mov_atipicos_Rprestamo as select * from tabla_temporal ")

df_recibos=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.co1504 as string) as co1504 , cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="262" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_recibos.map(lambda x: ((x[0],x[1]),x[2])).groupByKey().mapValues(list)
rdd_recibos_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_recibos_intervalos.map(lambda x: Row(str(x[0][0]),str(x[0][1]), str(x[1][0]), str(max(x[1][1],33.2))))
schema = StructType([StructField("titular", StringType(), True),StructField("co1504", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.mov_atipicos_Rseguros")
hc.sql("create table datalabin.mov_atipicos_Rseguros as select * from tabla_temporal ")

df_recibos=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.co1504 as string) as co1504 , cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="259" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_recibos.map(lambda x: ((x[0],x[1]),x[2])).groupByKey().mapValues(list)
rdd_recibos_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_recibos_intervalos.map(lambda x: Row(str(x[0][0]),str(x[0][1]), str(x[1][0]), str(max(x[1][1],3.1))))
schema = StructType([StructField("titular", StringType(), True),StructField("co1504", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.mov_atipicos_Rsociedades")
hc.sql("create table datalabin.mov_atipicos_Rsociedades as select * from tabla_temporal ")

df_recibos=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.co1504 as string) as co1504 , cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="252" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_recibos.map(lambda x: ((x[0],x[1]),x[2])).groupByKey().mapValues(list)
rdd_recibos_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_recibos_intervalos.map(lambda x: Row(str(x[0][0]),str(x[0][1]), str(x[1][0]), str(max(x[1][1],13))))
schema = StructType([StructField("titular", StringType(), True),StructField("co1504", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.mov_atipicos_Rtlfn")
hc.sql("create table datalabin.mov_atipicos_Rtlfn as select * from tabla_temporal ")

df_recibos=hc.sql('select cast(a.copemk as bigint) as titular, cast(a.co1504 as string) as co1504 , cast(a.imd2mx as double) as importe from standard.igv0_cp_ahcad_m a join  standard.mktbblon b ON a.copemk = b.copemk where trim(a.co1504)="257" and b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
rdd_clientes=df_recibos.map(lambda x: ((x[0],x[1]),x[2])).groupByKey().mapValues(list)
rdd_recibos_intervalos=rdd_clientes.map(lambda cliente_tupla: (cliente_tupla[0],mov_atipico(cliente_tupla[1])))
rdd_row=rdd_recibos_intervalos.map(lambda x: Row(str(x[0][0]),str(x[0][1]), str(x[1][0]), str(max(x[1][1],24.8))))
schema = StructType([StructField("titular", StringType(), True),StructField("co1504", StringType(), True),StructField("min", StringType(), True),StructField("max", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.mov_atipicos_Rvarios")
hc.sql("create table datalabin.mov_atipicos_Rvarios as select * from tabla_temporal ")

hc.sql("drop table if exists datalabin.eventos_rt_mov_recibos")
hc.sql("create table datalabin.eventos_rt_mov_recibos as select * from datalabin.mov_atipicos_ragua union all select * from datalabin.mov_atipicos_ralmacenes union all select * from datalabin.mov_atipicos_ralquileres union all select * from datalabin.mov_atipicos_rbeneficas union all select * from datalabin.mov_atipicos_rcolegio union all select * from datalabin.mov_atipicos_rcomunidades union all select * from datalabin.mov_atipicos_rgas union all select * from datalabin.mov_atipicos_rletra union all select * from datalabin.mov_atipicos_rlibrerias union all select * from datalabin.mov_atipicos_rluz union all select * from datalabin.mov_atipicos_rprestamo union all select * from datalabin.mov_atipicos_rseguros union all select * from datalabin.mov_atipicos_rsociedades union all select * from datalabin.mov_atipicos_rtlfn union all select * from datalabin.mov_atipicos_rvarios")
