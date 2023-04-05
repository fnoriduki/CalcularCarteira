import boto3
import io
import pandas as pd

client = boto3.client('cognito-identity',region_name="us-east-1")


def lambda_handler(event, context):
    
    
    ################## Leitura Credenciais ##################
    token = event['params']['header']['Authorization']
    
    response = client.get_id(
        IdentityPoolId="us-east-1:...",
        Logins={
         
            'cognito-idp.us-east-1.amazonaws.com/us-east-1...': token
        }                
    )
    identityId= response['IdentityId']
    print ('-----------------------------'+identityId)
    credenciais = client.get_credentials_for_identity(       
        IdentityId=identityId,                
        Logins={
           'cognito-idp.us-east-1.amazonaws.com/us-east-1...': token        
       }
    )
    
    aws_id = credenciais['Credentials']['AccessKeyId']
    aws_secret =credenciais['Credentials']['SecretKey']
    session_token=credenciais['Credentials']['SessionToken']

    session = boto3.Session(aws_access_key_id=aws_id, aws_secret_access_key = aws_secret, aws_session_token = session_token,region_name ="us-east-1")
    
    
    ################## Leitura Credenciais ##################
    ################## Leitura Objetos S3 ##################
    bucket_name = '4wallet-users'
    object_prefix = "users/"+identityId+"/"

    
    s3client = boto3.client('s3',aws_access_key_id=aws_id, aws_secret_access_key = aws_secret, aws_session_token = session_token,region_name ="us-east-1")
    obj = s3client.list_objects_v2(Bucket=bucket_name, Prefix= object_prefix)


    dataFrame = pd.DataFrame()
    for o in obj['Contents']:
        s3_obj = s3client.get_object(Bucket=bucket_name, Key= f"{o['Key']}")['Body'].read()        
        df = pd.read_excel(io.BytesIO(s3_obj),usecols=['Código de Negociação','Quantidade (Líquida)','Preço Médio (Compra)','Preço Médio (Venda)'] ,sheet_name = 0)        
        dataFrame = dataFrame.append(df)
      

    # print(dataFrame)
    ################## Leitura Objetos S3 ##################
    ################## Ajuste DataFrame ##################


    dataFrame.rename(columns={'Código de Negociação': 'ticker', 'Quantidade (Líquida)': 'qtd_liquida', 'Preço Médio (Compra)': 'pm_compra', 'Preço Médio (Venda)': 'pm_venda'}, inplace=True)

    print ('---------------------------------------')
    colunasRename = {'qtd_liquida':'qtd', 'pm_compra':'preco_medio','pm_venda':'preco_venda'}
    dataFrame=dataFrame.groupby('ticker', as_index=False).agg({'qtd_liquida':'sum', 'pm_compra':'mean','pm_venda':'mean'}).rename(columns=colunasRename)

    ################## Preparacao para Formato Dynamo ##################

    for i in dataFrame.columns:
        dataFrame[i] = dataFrame[i].astype(str)


    linhasCarteira=dataFrame.to_dict('records')
    carteira = dict()
    carteira["clientid"]= identityId #response['IdentityId'] 
    carteira["tickers"] = linhasCarteira
  

    ################## Gravacao Dynamo ##################
    dynamoClient= session.resource('dynamodb', region_name='us-east-1')
    table = dynamoClient.Table('tbCarteira')

    table.put_item( Item = carteira)
    # with table.batch_writer() as writer:       
    #         writer.put_item(Item=carteira)


    return {
        'statusCode': 200,
        'body': 'Calculo concluido'
    }
