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

def clasificar(valores):
	valores = [x for x in valores if str(x) != 'nan' and str(x) != 'inf']
	media = np.mean(valores)
	desv = (np.var(valores))**(0.5)
	variacion = 0
	if media == 0:
		variacion = 0
	else:
		variacion = desv/media
	c = 0
	if variacion < 0:
		c = 3
	elif variacion < 0.5:
		c = 1
	else:
		c = 2
	return c
def pendiente(datos):  #slope regression line  !!not->type(datos[i])==int p.t. i
	y = datos
	z = range(1, len(y)+1)
	z = [float(i) for i in z]
	b1 = ((sum([a*b for a, b in zip(z,y)])*len(z) - (sum(z)*sum(y)))/(sum(a**2 for a in z)*len(z) - (sum(z)**2)))
	return b1
def prediccion(movimientos):
	mov_ordenados=map(lambda x:x[0],sorted(movimientos,key=lambda x: x[1])) #importes ordenados por fecha
	valores = [x for x in mov_ordenados if str(x) != 'nan' and str(x) != 'inf']
	n = len(valores)
	alerta = False
	if n > 100:
		a = 0
		for i in range(1,5): #miro si en los últimos 5 días no ha habido un saldo negativo
			if valores[n-i] < 0:
				a += 1
		if a == 0: #si todos los valores recientes son mayores o iguales que cero
			dia_pred = valores[:-4] #quito los 4 ultimos elementos
			descubiertos = 0
			for j in dia_pred:
				if j < 0:
					descubiertos = descubiertos + j
			prediccion = dia_pred[len(dia_pred)-1] + pendiente(dia_pred)*0.5 + descubiertos*0.1
			if prediccion < 0:
				if valores[n-1] <= valores[n-2] <= valores[n-3] <= valores[n-4] and (np.mean([valores[n-1],valores[n-2],valores[n-3]])+1) < valores[n-4] and valores[n-1] + min(np.diff(valores)) < 0:
					alerta = True
	return alerta

# Spark context 
sc = SparkContext(appName = "Prediccion")
# Hive Context 
hc = HiveContext(sc)

df_movimientos=hc.sql('select cast(a.idprig as bigint) as cuenta,cast(a.imd2b6 as double) as saldo from standard.igv0_cp_ahca_d a join standard.mktbblon b  ON a.copemk = b.copemk where b.snapshot="LATEST" and b.cotpmk=1 and b.COH004=62120')
#df_movimientos=hc.sql('select cast(idprig as bigint) as cuenta,cast(imd2b6 as double) as saldo from standard.igv0_cp_ahca_d where cast(feigfi as timestamp) IS NULL')
rdd_movimientos=df_movimientos.map(lambda x: (x[0],x[1])).groupByKey().mapValues(list)
rdd_movimientos_clasificados=rdd_movimientos.map(lambda cliente_tupla: (cliente_tupla[0],clasificar(cliente_tupla[1])))
rdd_row=rdd_movimientos_clasificados.map(lambda x: Row(str(x[0]), str(x[1])))
schema = StructType([StructField("cuenta", StringType(), True),StructField("clasificacion", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal")
hc.sql("drop table if exists datalabin.eventos_rt_descubiertos_clasificados")
hc.sql("create table datalabin.eventos_rt_descubiertos_clasificados as select * from tabla_temporal ")

#....

df_movimientos=hc.sql('select cast(a.idprig as bigint) as cuenta,cast(a.imd2b6 as double) as saldo, cast(a.extractiondate as bigint) as fecha from standard.igv0_cp_ahca_d a join datalabin.eventos_rt_descubiertos_clasificados b on a.idprig = b.cuenta where b.clasificacion in ("2","3") ')
rdd_movimientos=df_movimientos.map(lambda x: (x[0],(x[1],x[2]))).groupByKey().mapValues(list)

rdd_movimientos_alertas=rdd_movimientos.map(lambda cliente_tupla: (cliente_tupla[0],prediccion(cliente_tupla[1])))
rdd_row=rdd_movimientos_alertas.map(lambda x: Row(str(x[0]), str(x[1])))
schema = StructType([StructField("cuenta", StringType(), True),StructField("alertas", StringType(), True)])
df = hc.createDataFrame(rdd_row, schema)
df.registerTempTable("tabla_temporal1")
hc.sql("drop table if exists datalabin.eventos_rt_descubiertos")
hc.sql("create table datalabin.eventos_rt_descubiertos as select * from tabla_temporal1 ")
