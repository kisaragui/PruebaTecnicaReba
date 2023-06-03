import pandas as pd
import sqlalchemy
from utils.LogHandler import logger 

def xlsx_to_sql(path: str, region: str, conn):
  """
   
Function that takes the information from the xlsx files and saves them in the mssql table.

  Parameters:
      path (str): mandatory
        The directory path where the directory where the xlsx files are located.
        
      region (str): mandatory
        The region name of xls file.
      
      conn (_type_): mandatory
        the engine conection of mssql instance.

  Returns:
      None
  """
  logger.info("---- Fhase 4: Processing data load to MSSQL")
      
  logger.info("---- Fhase 4: Read xlsx file: {}".format(region))  
    
  df = pd.read_excel("{}/{}.xlsx".format(path,region),parse_dates=[0])
  
  logger.info("---- Fhase 4: Apply transforms and cleanup ....") 
        
  df.rename( columns = {"Nivel general": "Nivel_General",	
                              "Alimentos y bebidas no alcohólicas": "Alimentos_Bebidas_No_alcoholicas",
                            "Pan y cereales": "Pan_y_cereales",
                            "Carnes y derivados": "Carnes_y_derivados",
                            "Leche, productos lácteos y huevos": "Leche_productos_lacteos_y_huevos",
                            "Aceites, grasas y manteca"	: "Aceites_grasas_y_manteca",
                            "Verduras, tubérculos y legumbres": "Verduras_tuberculos_y_legumbres",
                            "Azúcar, dulces, chocolate, golosinas, etc.": "Azucar_dulces_chocolate_golosinas",
                            "Bebidas no alcohólicas": "Bebidas_no_alcoholicas",
                            "Café, té, yerba y cacao": "Cafe_te_yerba_y_cacao",
                            "Aguas minerales, bebidas gaseosas y jugos": "Aguas_minerales_bebidas_gaseosas_y_jugos",
                            "Bebidas alcohólicas y tabaco": "Bebidas_alcoholicas_y_tabaco",
                            "Bebidas alcohólicas": "Bebidas_alcoholicas",
                            "Prendas de vestir y calzado": "Prendas_de_vestir_y_calzado",
                            "Prendas de vestir y materiales": "Prendas_de_vestir_y_materiales",
                            "Vivienda, agua, electricidad, gas y otros combustibles": "Vivienda_agua_electricidad_gas_y_otros_combustibles",
                            "Alquiler de la vivienda y gastos conexos": "Alquiler_de_la_vivienda_y_gastos_conexos",
                            "Alquiler de la vivienda": "Alquiler_de_la_vivienda",
                            "Mantenimiento y reparación de la vivienda": "Mantenimiento_y_reparacion_de_la_vivienda",
                            "Electricidad, gas y otros combustibles": "Electricidad_gas_y_otros_combustibles",
                            "Equipamiento y mantenimiento del hogar": "Equipamiento_y_mantenimiento_del_hogar",
                            "Bienes y servicios para la conservación del hogar": "Bienes_y_servicios_para_la_conservacion_del_hogar",
                            "Productos medicinales, artefactos y equipos para la salud": "Productos_medicinales_artefactos_y_equipos_para_la_salud",
                            "Gastos de prepagas": "Gastos_de_prepagas",
                            "Adquisición de vehículos": "Adquisicion_de_vehiculos",
                            "Funcionamiento de equipos de transporte personal": "Funcionamiento_de_equipos_de_transporte_personal",
                            "Combustibles y lubricantes para vehículos de uso del hogar": "Combustibles_y_lubricantes_para_vehiculos_de_uso_del_hogar",
                            "Transporte público": "Transporte_publico",
                            "Comunicación": "Comunicacion",
                            "Servicios  de telefonía e internet": "Servicios_de_telefonia_e_internet",
                            "Recreación y cultura": "Recreacion_y_cultura",
                            "Equipos audiovisuales, fotográficos y de procesamiento de la información": "Equipos_audiovisuales_fotográficos_y_de_procesamiento_de_la_informacion",
                            "Servicios recreativos y culturales": "Servicios_recreativos_y_culturales",
                            "Periódicos, diarios, revistas, libros y artículos de papelería": "Periodicos_diarios_revistas_libros_y_articulos_de_papeleria",
                            "Educación": "Educacion",
                            "Restaurantes y hoteles": "Restaurantes_y_hoteles",
                            "Restaurantes y comidas fuera del hogar": "Restaurantes_y_comidas_fuera_del_hogar",
                            "Bienes y servicios varios": "Bienes_y_servicios_varios",
                            "Cuidado personal": "Cuidado_personal"},
                            inplace = True)
  
  df_clean= df.replace("///", 0)

  logger.info("---- Fhase 4: write Region name ....") 
  
  df_clean["Region_name"] = region
  df_final = df_clean.to_sql("INDICES_APERTURA", con=conn, if_exists="append",index=False, dtype={'Fecha': sqlalchemy.DateTime})
  
  logger.info("---- Fhase 4: Load {} Rows to table 'INDICES_APERTURA'".format(len(df_clean))) 
      
  return df_final