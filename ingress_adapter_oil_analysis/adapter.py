"""
Ingress adapter for Oil Analysis data
"""
import json
from typing import Optional
import pandas

from osiris.adapters.ingress_adapter import IngressAdapter
from osiris.core.configuration import Configuration

configuration = Configuration(__file__)
config = configuration.get_config()
logger = configuration.get_logger()


class OilAnalysisAdapter(IngressAdapter):
    """
    Transforming XLSX to json format
    """
    # pylint: disable=too-many-arguments
    def __init__(self, ingress_url: str, tenant_id: str, client_id: str, client_secret: str, dataset_guid: str):
        super().__init__(ingress_url, tenant_id, client_id, client_secret, dataset_guid)

        self.excel_file = config['Excel']['file']
        self.sheet1 = config['Excel']['sheet1']
        # self.sheet2 = config['Excel']['sheet2']
        # self.sheet3 = config['Excel']['sheet3']
        # self.sheet4 = config['Excel']['sheet4']
        # self.sheet5 = config['Excel']['sheet5']

    def retrieve_data(self) -> bytes:
        data_sheet1 = self.__transform_sheet(self.excel_file, self.sheet1, col_y=52)
        # data_sheet2 = self.__transform_sheet(self.excel_file, self.sheet2, header_row=3, row_y=3031, col_y=18)
        # data_sheet3 = self.__transform_sheet(self.excel_file, self.sheet3, header_row=3, row_y=1429, col_y=5)
        # data_sheet4 = self.__transform_sheet(self.excel_file, self.sheet4, header_row=2, row_y=4155, col_y=52)
        # data_sheet5 = self.__transform_sheet(self.excel_file, self.sheet5, header_row=3)

        return data_sheet1

    # pylint: disable=too-many-arguments
    def __transform_sheet(self,
                          file: str,
                          sheet: str,
                          header_row: int = 0,
                          row_x: Optional[int] = None,
                          row_y: Optional[int] = None,
                          col_x: Optional[int] = None,
                          col_y: Optional[int] = None) -> bytes:
        """
        Input XLSX file, format dates, and transform to JSON.
        :param file: The path to the XLSX file.
        :param sheet: The name of the sheet in the XLSX file.
        :param header: The row the headers start at.
        :param row_x: The row the data begins at.
        :param row_y: The row the data ends at.
        :param col_x: The first column in the sheet.
        :param col_x: The last column in the sheet.
        """
        logger.info('Running transformation.')

        dataframe = pandas.read_excel(file, sheet_name=sheet, engine='openpyxl', header=header_row)

        dataframe = dataframe.iloc[row_x:row_y, col_x:col_y]

        # Format datetime
        for col_name in dataframe.columns:
            if 'dato' in col_name or 'date' in col_name:
                dataframe[col_name] = pandas.to_datetime(dataframe[col_name], format='%d-%m-%Y')
                dataframe[col_name] = dataframe[col_name].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        if sheet == self.sheet1:
            dataframe['pCH4'] = dataframe['CH4_µL/L'] / \
                (dataframe['CH4_µL/L'] + dataframe['C2H4_µL/L'] + dataframe['C2H2_µL/L'])
            dataframe['pC2H4'] = dataframe['C2H4_µL/L'] / \
                (dataframe['CH4_µL/L'] + dataframe['C2H4_µL/L'] + dataframe['C2H2_µL/L'])
            dataframe['pC2H2'] = dataframe['C2H2_µL/L'] / \
                (dataframe['CH4_µL/L'] + dataframe['C2H4_µL/L'] + dataframe['C2H2_µL/L'])

        dataframe_dict = dataframe.to_dict(orient='records')

        logger.info('Finished running transformation.')

        return json.dumps(dataframe_dict).encode('UTF-8')


def main():
    """
    Initialize class and upload JSON data
    """
    ingress_url = config['Azure Storage']['ingress_url']
    dataset_guid = config['Datasets']['source']
    tenant_id = config['Authorization']['tenant_id']
    client_id = config['Authorization']['client_id']
    client_secret = config['Authorization']['client_secret']

    transform = OilAnalysisAdapter(ingress_url, tenant_id, client_id, client_secret, dataset_guid)

    print(transform.retrieve_data())
    # transform.upload_json_data(schema_validate=False)


if __name__ == '__main__':
    main()
