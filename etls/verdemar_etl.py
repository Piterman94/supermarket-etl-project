from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, date
import requests
import pandas as pd
from io import StringIO

def run_etl_process():

    base_url = "https://api-loja.loja.verdemaratevoce.com.br"
    headers = {
        'Accept': 'application/json',
        'Authorization': f"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJ2aXBjb21tZXJjZSIsImF1ZCI6ImFwaS1hZG1pb"
                         f"iIsInN1YiI6IjZiYzQ4NjdlLWRjYTktMTFlOS04NzQyLTAyMGQ3OTM1OWNhMCIsInZpcGNvbW1lcmNlQ2xpZW50ZUlkIj"
                         f"pudWxsLCJpYXQiOjE3MTAxODQyNjQsInZlciI6MSwiY2xpZW50IjpudWxsLCJvcGVyYXRvciI6bnVsbCwib3JnIjoiNzQ"
                         f"ifQ.Kzu5nBtVmBjpU5QreytgmxyL7gS4zk1oqnM6BH6GfZyDBefxCtkAkURUovnzqPaF3BAyRGopSjRB5Ct9zEf1qw",

        'Content-Type': 'application/json',
        'Dnt': '1',
        'Organizationid': '74',
        'Origin': 'https://www.loja.verdemaratevoce.com.br',
        'Referer': 'https://www.loja.verdemaratevoce.com.br/',
    }

    successful_sections = []
    for s in range(0, 320):
        response = requests.get(
            f"{base_url}/v1/loja/classificacoes_mercadologicas/secoes/{s}/produtos/filial/1/centro_distribuicao/1"
            f"/ativos?orderby=produto.descricao:asc",
            headers=headers
        ).json()
        print(successful_sections)
        if response['data']:
            successful_sections.append(s)

    temp_data = []
    for s in successful_sections:
        print(f"Processing section: {s}")
        initial_response = requests.get(
            f"{base_url}/v1/loja/classificacoes_mercadologicas/secoes/{s}/produtos/filial/1/centro_distribuicao/1"
            f"/ativos?orderby=produto.descricao:asc&page=1",
            headers=headers
        ).json()

        total_pages = initial_response['paginator']['total_pages']
        for page in range(1, total_pages + 1):
            response = requests.get(
                f"{base_url}/v1/loja/classificacoes_mercadologicas/secoes/{s}/produtos/filial/1/centro_distribuicao/1"
                f"/ativos?orderby=produto.descricao:asc&page={page}",
                headers=headers
            ).json()

            for product in response["data"]:
                product_info = {
                    "product_id": product["produto_id"],
                    "classification_id": product["classificacao_mercadologica_id"],
                    "name": product["descricao"],
                    "price": product["preco"],
                    "in_offer": product["em_oferta"],
                    "offer_price": product["oferta"]["menor_preco"] if product["em_oferta"] else None,
                    "unit": product["unidade_sigla"],
                    "measure": product.get("unidade_fracao", {}).get("sigla", None),
                    "quantity": product.get("unidade_fracao", {}).get("quantidade", None)
                }
                temp_data.append(product_info)

    temp_df = pd.DataFrame(temp_data)
    csv_buffer = StringIO()
    temp_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'verdemar-dataset'
    file_key = f'products_{date.today().strftime("%Y%m%d")}.csv'


    s3_hook.load_bytes(
        bytes_data=csv_buffer.getvalue().encode('utf-8'),
        bucket_name=bucket_name,
        key=file_key,
        replace=True
    )
    print(f"File uploaded successfully to {bucket_name} as {file_key}")