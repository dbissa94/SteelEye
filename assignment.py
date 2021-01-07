import traceback
import zipfile
from xml.etree import ElementTree as ET

import boto3
import io
import pandas as pd
import requests

ZIPPED_S3_BUCKET = 'lambda-test-bucket-s3-to-es'
XML_S3_BUCKET = 'lambda-test-bucket-s3-to-es'
CSV_S3_BUCKET = 'lambda-test-bucket-s3-to-es'


class XmlParser:
    s3_resource = boto3.resource('s3')

    def __init__(self, link):
        self.link = link

    def read_xml(self):
        response_result = []
        download_link = ''
        try:
            xml = requests.get(self.link).content.decode('utf-8')
            root = ET.fromstring(xml)
            for elem in root.iter('doc'):
                dict_object = {}
                for child in elem:
                    dict_object[child.attrib.get('name')] = child.text
                response_result.append(dict_object)
            for final in response_result:
                if final['file_type'] == 'DLTINS':
                    download_link = final['download_link']
                    break
            return download_link
        except Exception as error:
            traceback.print_exc(error)

    def extract_zip_file(self):
        """
        Zip File To Upload on S3
        :return: file_name of uploaded file on s3
        """
        download_link = self.read_xml()
        file_name = download_link.split('/')[-1]
        file_downloaded = requests.get(download_link, stream=True)
        self.s3_resource.meta.client.upload_fileobj(io.BytesIO((file_downloaded.content)),
                                                    ZIPPED_S3_BUCKET, file_name)
        print("==============ZIP File Uploaded Successfully=========")

        return file_name

    def extract_xml_to_s3(self):
        """
        extract zip to another bucket on s3 to store the xml files
        :return: end s3 url
        """
        file_name = self.extract_zip_file()
        print(file_name)
        zip_obj = self.s3_resource.Object(bucket_name=ZIPPED_S3_BUCKET, key=file_name)
        buffer = io.BytesIO(zip_obj.get()["Body"].read())
        zipped_file = zipfile.ZipFile(buffer)
        for filename in zipped_file.namelist():
            self.s3_resource.meta.client.upload_fileobj(
                zipped_file.open(filename),
                Bucket=XML_S3_BUCKET,
                Key=f'{filename}'
            )
        print("==============XML File Extracted Successfully=========")
        return f"https://lambda-test-bucket-s3-to-es.s3-ap-southeast-1.amazonaws.com/{file_name.split('.')[0]}.xml"

    def process_file(self):
        xml_file_path = self.extract_xml_to_s3()
        xml_file_name = xml_file_path.split('/')[-1]
        obj = self.s3_resource.Object(XML_S3_BUCKET, xml_file_name)
        body = obj.get()['Body'].read().decode('utf-8')
        root = ET.fromstring(body)
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
        print("==============CSV Data Frame Generated Successfully=========")
        return result_df, xml_file_name

    def upload_xml_to_csv_to_s3(self):
        processed_data = self.process_file()
        data_frame = processed_data[0]
        file_name = f"{processed_data[1].split('.')[0]}.csv"
        bucket = CSV_S3_BUCKET  # already created on S3
        csv_buffer = io.StringIO()
        data_frame.to_csv(csv_buffer)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, file_name).put(Body=csv_buffer.getvalue())
        # After this we can delete the other csv and xml files


def event_handler(event, context):
    parser_obj = XmlParser(
        link="https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2020-01-08T00:00:00Z+TO+2020-01-08T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100")
    parser_obj.upload_xml_to_csv_to_s3()
    return "Success"


# if __name__ == "__main__":
#     event_handler(event=None, context=None)
