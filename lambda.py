import boto3
import time

def lambda_handler(event, context):
    glue_client = boto3.client('glue')
    crawler_name = 'crawler-bovespa-raw'
    job_name = 'glue_bovespa'

    try:
        print(f"Iniciando o Crawler: '{crawler_name}'")
        glue_client.start_crawler(Name=crawler_name)
        print("Aguardando o Crawler terminar...")
        while True:
            response = glue_client.get_crawler(Name=crawler_name)
            crawler_state = response['Crawler']['State']
            
            if crawler_state == 'READY':
                print("Crawler finalizado com sucesso!")
                break
            elif crawler_state == 'FAILED':
                print(f"O Crawler falhou. Verifique os logs do Crawler para mais detalhes.")
                raise Exception(f"Crawler '{crawler_name}' falhou.")
            
            time.sleep(15)

        print(f"Iniciando a execução do Job do Glue: '{job_name}'")
        job_run_response = glue_client.start_job_run(JobName=job_name)
        job_run_id = job_run_response['JobRunId']
        print(f"Sucesso! JobRunId: {job_run_id}")
        
        return {
            'statusCode': 200,
            'body': f'Crawler e Job {job_name} iniciados com sucesso. Run ID: {job_run_id}'
        }

    except Exception as e:
        print(f"Erro no processo de automação: {str(e)}")
        raise e