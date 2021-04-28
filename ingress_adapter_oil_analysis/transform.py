from osiris.adapters.ingress_adapter import IngressAdapter

import configparser
import pandas
import json
import pprint


class Transform(IngressAdapter):
    """
    Transforming XLSX to json format
    """
    def retrieve_data(self) -> bytes:
        config = configparser.ConfigParser()
        config.read('conf.ini')

        excel_file = config['Excel']['file']
        sheet1 = config['Excel']['sheet1']
        sheet2 = config['Excel']['sheet2']
        sheet3 = config['Excel']['sheet3']
        sheet4 = config['Excel']['sheet4']
        sheet5 = config['Excel']['sheet5']
        
        data_sheet1 = self.transform_sheet(excel_file, sheet1, 0, 0, None, 0, 52)

        #data_sheet2 = self.transform_sheet(excel_file, sheet2, 2, 2, 3033, 0, 18)

        #data_sheet3 = self.transform_sheet(excel_file, sheet3, 2, 2, 1431, 0, 5)

        #data_sheet4 = self.transform_sheet(excel_file, sheet4, 1, 1, 4157, 0, 52)

        #data_sheet5 = self.transform_sheet(excel_file, sheet5, 2, 2, 98, 0, 52)

        return data_sheet1

    def transform_sheet(self, file: str, sheet: str, header: int, row_x: int, row_y: int, col_x: int, col_y: int):
        """
        :param file: The path to the XLSX file.
        :param sheet: The name of the sheet in the XLSX file.
        :param header: The row the headers start at.
        :param row_x: The row the data begins at.
        :param row_y: The row the data ends at.
        :param col_x: The first column in the sheet.
        :param col_x: The last column in the sheet.
        """
        df = pandas.read_excel(file, sheet_name=sheet, engine='openpyxl')

        if header != 0:
            df.columns = df.iloc[header]
            df = df.drop(header)
            df = df.reset_index(drop=True)
        
        df = df.iloc[row_x:row_y, col_x:col_y]
        
        # Format datetime
        for col_name in df.columns:
            if 'dato' in col_name:
                df[col_name] = pandas.to_datetime(df[col_name], format='%d-%m-%Y')
                df[col_name] = df[col_name].dt.strftime('%d-%m-%Y')

        df_dict = df.to_dict(orient='records')
        
        return json.dumps(df_dict).encode('UTF-8')


def main():
    config = configparser.ConfigParser()
    config.read('conf.ini')

    ingress_url = 'https://dp-test.westeurope.cloudapp.azure.com/osiris-ingress' # 'http://localhost:8000' #config['Azure Storage']['account_url']
    filesystem_name = config['Azure Storage']['filesystem_name']

    dataset_guid = config['Datasets']['source']
    #dataset_guid = config['Datasets']['destination']
    date_format = config['Datasets']['date_format']
    date_key_name = config['Datasets']['date_key_name']

    tenant_id = config['Authorization']['tenant_id']
    client_id = config['Authorization']['client_id']
    client_secret = config['Authorization']['client_secret']

    transform = Transform(ingress_url, tenant_id, client_id, client_secret, dataset_guid)

    print(transform.retrieve_data())
    #transform.upload_json_data(schema_validate=False)

if __name__ == '__main__':
    main()
