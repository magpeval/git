import apache_beam as beam
from beam_nuggets.io import relational_db


#configuracion de target
db_config = relational_db.SourceConfiguration(
    drivername='postgresql',
    host='localhost',
    port=5432,
    username='postgres',
    password='postgres',
    database='postgres',
)

table_companies = relational_db.TableConfiguration(
    name='companies',
    create_if_missing=True,
    primary_key_columns=['company_id']
)


table_charges = relational_db.TableConfiguration(
    name='charges',
    create_if_missing=True,
    primary_key_columns=['id']
)
p = beam.Pipeline()

def imprime(row):
    print(row)
    return row

class PrepareDataCompanies(beam.DoFn):
    def process(self, element):
        row = {}
        row['company_id'] = str(element['company_id'])
        row['company_name'] = str(element['name'])
        if row['company_name'] == '':
            row['company_name']  = 'default-name-'+ str(row['company_id'])
        #retorno async
        yield (row['company_name'] +'_'+str(row['company_id']))


class PrepareDataCharges(beam.DoFn):
    def process(self, element):
        #proceso de id
        del element['_id']
        row = {}
        row['company_id'] = element['company_id']
        row['created_at'] = element['created_at']
        row['id'] = str(element['id']).rstrip().lstrip()
        row['status'] = element['status']

        #verifico los montos que sean flotantes
        try:
            row['amount'] = 0 if element['amount'] == '' else float(element['amount'])
            row['amount'] = round(element['amount'] , 6)
        except:
            row['amount'] = 0
    
        #renombro el camo de actualizacion
        row['updated_at'] = element['paid_at']
        if row['updated_at'] == '':
            row['updated_at'] = None

        #retorno async
        yield row


main = (
        p
        |'Data parquet'>> beam.io.ReadFromParquet("./output/data/part-00000-ceb8f4d8-0ad2-4f53-9b18-37a272eb8972-c000.snappy.parquet")
        #|'imprime' >> beam.Map(imprime)         

)

companies =(
        main
        |'Filtro por compañia'>> beam.Filter(lambda row : len(str(row['company_id'])) > 24) 
        |'Prepara informacion companies' >> beam.ParDo(PrepareDataCompanies())
        |'Agrupación por elemento' >> beam.combiners.Count().PerElement()
        |'Split de campos unicos' >> beam.Map(lambda row : row[0].split('_'))
        |'Preparación de elemento' >> beam.Map(lambda row : {'company_id':row[1] , 'company_name' : row[0]})        
        |'Filtro por nombre de compañia'>> beam.Filter(lambda row : str(row['company_name']) != 'None') 
        |'Filtro por nombre de compañia quitando campo 0 '>> beam.Filter(lambda row :  '0' not in str(row['company_name'])) 
        |'Escribir a DB table companies' >> relational_db.Write(
                                                source_config=db_config,
                                                table_config=table_companies
                                                    )
        
)


charges =(
        main
        |'Filtro por comañia charges'>> beam.Filter(lambda row : len(str(row['company_id'])) > 24) 
        |'Prepara informacion charges' >> beam.ParDo(PrepareDataCharges())
        |'Filtrado que el monto para que no sea infinito' >> beam.Filter( lambda row : row['amount'] != float('inf'))       
        |'Imprime' >> beam.Map(imprime) 
        |'FIltrado de id no vacio'>> beam.Filter(lambda row : str(row['id']) != '') 
        |'Escribir a DB table charges' >> relational_db.Write(
                                                source_config=db_config,
                                                table_config=table_charges
                                                    )
        
)

p.run().wait_until_finish()

