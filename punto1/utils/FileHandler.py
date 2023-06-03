import requests as r
from utils.LogHandler import logger
import os
import pandas as pd

class FileHandler:
    
    """
        This class is used to automate the download process of the excel file
    
        Return:
            None

    """   
    
    def pathdir(seff):
        
        """
        Return the current directory of project

        Returns:
            directory: Directory absolute  actual
        """
        
        directory = os.getcwd()
        
        return directory
    
    def get_xls(self, url : str):
        
        """
            A function for a function to download the excel file and save it in the system.

        Parameters:
            url (str): obligatory.
                Link absolute of excel file in site web.

        Returns:
            msg : str.
                ok: All good.
                failed: Execution error.
        """
        
        logger.info("---- Phase 1: Download xls Started ----")
        msg = ""
        
        path = os.path.join(self.pathdir(), "data")
        filename = url.rsplit('/', 1)[-1]
        response = r.get(url)        
        try:
            if "excel" in response.headers['content-type']:
            
                logger.info("---- Fhase 1: Sucessfully fetched the Excel!!")
            
                if not os.path.isfile('{}/{}'.format(path,filename)):
                
                    with open('{}/{}'.format(path,filename), 'wb') as f:
                        f.write(response.content)
                        f.close()
                    
                    msg = "ok"
                    logger.info("---- Fhase 1: File: {} created and this located in {}".format(filename,path))                 
                else:
                    msg = "ok" 
                    logger.info("---- Fhase 1: File: {} existed...".format(filename))
                    logger.info("---- Phase 1: Download xls Finished ----")
                
            else:
                msg = "failed"  
                logger.error("---- Fhase 1: Content-Type: {} not is Excel format ".format(response.headers['content-type']))
                logger.info("---- Phase 1: Download xls Interrupted ----")
        
        except Exception as err:
            msg = "failed"
            logger.error(err)
            logger.info("---- Phase 1: Download xls Interrupted ----")    
            return msg
        
        if msg == "ok": 
            
            logger.info("---- Phase 1: Download xls Finished ----")
            return msg
        else:
            msg = "failed"
            logger.error("---- Phase 1: Download xls Interrupted ----")        
            return msg
        
    
    def get_cheatsheet(self, file: str, cheatsheet_name: str): 
        
        """
            A function that returns the content of the excel 
            sheet and extracts the regions by parts.

        Parameters:
            file (str):  obligatory.
                The name with extension of file excel.
                
            cheatsheet_name (str): obligatory.
                The name of sheet of file excel to extract.
                
        Returns:
            msg : str.
                ok: All good.
                failed: Execution error.
                
        """
        
        logger.info("---- Phase 2: Transform xls Started ----")
        
        regions_name = ["Región_GBA", 
                        "Región_Pampeana", 
                        "Región_Noroeste", 
                        "Región_Noreste", 
                        "Región_Cuyo",
                        "Región_Patagonia"]
        msg = ""
        path = os.path.join(self.pathdir(), "data")
    
        file_exist = os.path.isfile('{}/{}'.format(path,file))
        
        try: 
            if file_exist:   
                logger.info("---- Phase 2: Reading xls....")
                
                count_skips = 4
                i = 0
                skipedrows= lambda x: x in range(0, count_skips)
                chunksize_r_gba = 51
                chunksize_r_others = 48
                
                logger.info("---- Phase 2: Batch processing xls data ....")
                logger.info("---- Phase 2: Batch: {} chunks".format(len(regions_name)))
                
                while i < len(regions_name):

                    if regions_name[i] == "Región_GBA":
                        
                        logger.info("---- Phase 2: Batch: {}".format(i + 1))
                        
                        msg= self.clean_df(regions_name[i], path, file, cheatsheet_name, skipedrows, chunksize_r_gba)
                        count_skips+=chunksize_r_gba
                        i+=1
                        
                    else:
                        logger.info("---- Phase 2: Batch: {}".format(i + 1))
                        
                        msg= self.clean_df(regions_name[i], path, file, cheatsheet_name, skipedrows, chunksize_r_others)
                        count_skips+=chunksize_r_others + 1
                        i+=1
            else:
                msg = "failed"
                logger.error("---- Phase 2: Location: {} not exist".format(file_exist))
                logger.error("---- Phase 2: Transform xls Interrupted ----")
                

            if msg == "ok" and i == 6:
                logger.info("---- Phase 2: xls Process Completed")
                logger.info("---- Phase 2: Transform xls Finished ----")
                    
            else:  
                msg = "failed"
                logger.error("---- Phase 2: Transform xls Interrupted ----") 
                       
        except Exception as err:
            msg = "failed"
            logger.error(err)
            logger.error("---- Phase 2: Transform xls Interrupted ----")       
    
        return msg
      
                
    def clean_df(self,regions_name: str , path: str, file: str,cheatsheet_name: str , skipedrows: int, chunksize: int): 
        """
            This function is responsible for reading the excel file 
            and the sheet to then save the region passed.

        Parameters:
            regions_name (str): obligatory.
                The region name to extract.
                
            path (str): obligatory.
                This path absolute to read the excel.
                
            file (str): obligatory.
                The name with extension of file excel.
                
            cheatsheet_name (str): obligatory.
             This sheet name is that through which the information will be extracted.
            
            skipedrows (int): obligatory.
                This number especific of rows to skipped.
            
            chunksize (int): obligatory.
                This number of rows containing the region information.
            
        Returns:
            msg : str.
                ok: All good.
                failed: Execution error.
                
        """
        msg = ""        
        logger.info("---- Phase 2: Processing... ")
        file_exist = os.path.isfile('{}/{}.xlsx'.format(path,regions_name))
        
        if not file_exist:
            
            df = pd.read_excel('{}/{}'.format(path,file),
                                sheet_name=cheatsheet_name,skiprows=skipedrows,
                                nrows= chunksize,
                                header=None)
            
            logger.info("---- Phase 2: Applying transformations and cleansing on the data")
            
            df_dropped_na = df.dropna(axis=0)
            df_trasporsed = df_dropped_na.T
            df_trasporsed.iloc[0,0] = "Fecha"
            df_trasporsed.iloc[1:,0] = pd.to_datetime(df_trasporsed.iloc[1:,0],format="%Y/%m/%d").astype(str)
            df_trasporsed.to_excel('{}/{}.xlsx'.format(path,regions_name),
                                                float_format="%.1f",
                                                header=False,
                                                index=False)
            
            msg = "ok"
            
        else:
            logger.info("---- Phase 2: Batch processed ")
            msg = "ok"

        return msg
