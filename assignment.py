import configparser
import os
import traceback
from xml.etree import ElementTree as ET

import boto3
import io
import pandas as pd
import requests
import zipfile


class XmlParser:

    def __init__(self, link):
        self.link = link

    def read_xml(self):
        response_result = []
        try:
            print(self.link)
            xml = requests.get(self.link).content.decode('utf-8')
            root = list(ET.fromstring(xml).find('result'))
            for child in root:
                doc_dict = {}
                for element in child:
                    key = element.attrib.get('name')
                    doc_dict[key] = element.text
                    if doc_dict.get('file_type') == 'DLTINS':
                        response_result.append(doc_dict)
                        break
                break
            return response_result
        except Exception as error:
            traceback.print_exc(error)

    def download_file(self):
        download_link_result = self.read_xml()
        try:
            download_link = download_link_result[0].get('download_link')
            dir_path = os.path.dirname(os.path.realpath(__file__))
            print("File is Downloading.......")
            file_downloaded = requests.get(download_link, stream=True)
            zipped_file = zipfile.ZipFile(io.BytesIO(file_downloaded.content))
            zipped_file.extractall(dir_path)
            extracted = zipped_file.namelist()
            zipped_file.close()
            print("File Extraction is Completed......")
            extracted_file = os.path.join(dir_path, extracted[0])
            return extracted_file
        except Exception as error:
            print(error)

    # def extract_xml(xml_path):

    def process_file(self):
        file_path = self.download_file()
        root = ET.parse(file_path).getroot()
        # print([elem.tag for elem in root.iter()])
        result_to_csv = []
        for record in root.iter('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}ModfdRcrd'):
            final_dict = {}
            for data in list(record)[:2]:
                key = data.tag.split('}')[1]
                if key == "FinInstrmGnlAttrbts":
                    for entry in list(data):
                        final_dict[entry.tag.split('}')[1]] = entry.text
                    final_dict.pop('ShrtNm')
                if key == "Issr":
                    final_dict[key] = data.text
            result_to_csv.append(final_dict)
        result_df = pd.DataFrame(result_to_csv)
        result_df.to_csv('t.csv', index=False)
        return result_df, os.path.basename(file_path)

    def upload_to_s3(self):
        processed_data = self.process_file()
        data_frame = processed_data[0]
        file_name = f'{processed_data[1]}.csv'
        bucket = ''  # already created on S3
        csv_buffer = io.StringIO()
        data_frame.to_csv(csv_buffer)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, file_name).put(Body=csv_buffer.getvalue())


if __name__ == "__main__":
    config = configparser.ConfigParser(interpolation=None)
    config.read('.aws/credentials')
    parser_obj = XmlParser(link=config['default']['link'])
    parser_obj.upload_to_s3()
