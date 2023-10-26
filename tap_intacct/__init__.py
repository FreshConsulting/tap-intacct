import json
import sys
import singer
import random 

from singer import metadata
from tap_intacct.discover import discover_streams, discover_streams_for_discover_mode
from tap_intacct.sync import sync_stream
from tap_intacct import s3

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "bucket", "company_id"]

def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams_for_discover_mode(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def do_sync(config, catalog, state):
    LOGGER.info('Starting sync.')

    for stream in catalog['streams']:

        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata']) 
        '''
        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue
        '''
        singer.write_state(state)
        key_properties = metadata.get(mdata, (), 'table-key-properties') or []
        singer.write_schema(stream_name, stream['schema'], key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(config, state, stream)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value) #Esta la ocupo 

    LOGGER.info('Done syncing.')


@singer.utils.handle_top_exception(LOGGER)
def main():
    
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    csv_files_names_bucket = [] #Aqui guardo los nombres de todos los CSV files que hay en el bucket
    #LOGGER.info(args)
    #LOGGER.info(args.properties)
    try:
        #LOGGER.info('Entre al TRY')
        for csv_files in s3.list_files_in_bucket(config['bucket']): #The data that get here is in the Docs
                #Esta parte la puedo aprovechar para llamar directo a la funcion de sync, pasando 
                #directamente el nombre de la tabla, que es la que estoy obteniendo aqui linea
                splitChar1 = '/'
                splitChar2 = '.csv'
                csv_file_name_found = (csv_files['Key'].split(splitChar1)[1]).split(splitChar2)[0]
                csv_files_names_bucket.append(csv_file_name_found)
                
        LOGGER.warning("I have direct access to the bucket without assuming the configured role.")
    except:
        s3.setup_aws_client(config)


    files = [
        'APDETAIL.change.27838.2023-08-18_08.25.31_UTC_cr_00000',
        'APDETAIL.change.28042.2023-08-22_08.22.11_UTC_cr_00000',
        'APDETAIL.change.28093.2023-08-23_08.30.45_UTC_cr_00000',
        'ARRECORD.change.29864.2023-09-27_08.22.08_UTC_cr_00000',
        'ARRECORD.change.29864.2023-09-27_08.22.09_UTC_upd_00001',
        'ARRECORD.change.30170.2023-10-03_08.24.55_UTC_upd_00001'
    ]
    
    lista2 = []
    str1 = 'SODOCUMENT'
    for i in range(0,len(csv_files_names_bucket)):
        if str1 in csv_files_names_bucket[i]:
            lista2.append(csv_files_names_bucket[i])
        #lista2.append(list(filter(lambda x: str1 in x, csv_files_names_bucket[i] )))
    
    LOGGER.info(lista2)

    #listaAux = []
    #for i in range(0,len(lista2)):
    #    listaAux.append(lista2[i][0])
    
    lista = []
    if len(lista2) > 10:
        lista = lista2[:10] #Para omitir los ALL [1:10]
    else:
        lista = lista2 #[1:] #Para omitir los ALL
 
    LOGGER.info(lista)
    #lista = list(filter(lambda x: "SODOCUMENTSUBTOTALS.change.30282.2023-10-05_08.23.58_UTC_cr_00000" in x, csv_files_names_bucket ))
    #Aqui inicia
    #Prueba diferentes tablas, que agarre 10/10 
    '''
    amount_sub_list = 10

    csv_files_names_for_catalog = []
    csv_files_names_for_catalog = [
        csv_files_names_bucket[name_file:name_file+amount_sub_list] for name_file in range(0, len(csv_files_names_bucket),amount_sub_list)
    ]
    
    lista_prueba = []
    lista_prueba.append(csv_files_names_for_catalog[0])
    for n in range(0,9):
        lista_prueba.append(csv_files_names_for_catalog[random.randrange(5,200,20)])
    #Esto lo uso para la prueba de diferentes tablas 
    
    lista = []
    for sublist in lista_prueba:
        for item in sublist:
            lista.append(item)
    #Hasta aqui
    '''
    if args.discover:
        LOGGER.debug("Running discovery mode...")
        do_discover(args.config)
    else:
        streams = discover_streams(config, lista)
        if not streams:
            raise Exception("No streams found")
        catalog = {"streams": streams}
        
        do_sync(config, catalog, args.state)
    
    ############# Esto fue lo que sirvio ##################
    '''
    amount_sub_list = 80

    csv_files_names_for_catalog = [
        csv_files_names_bucket[name_file:name_file+amount_sub_list] for name_file in range(0, len(csv_files_names_bucket),amount_sub_list)
    ]
    
    for tables in csv_files_names_for_catalog:
        streams = discover_streams(config, tables)
        if not streams:
            raise Exception("No streams found")
        catalog = {"streams": streams}

        do_sync(config, catalog, args.state)
    
    ################################################
    '''

if __name__ == '__main__':
    main()

