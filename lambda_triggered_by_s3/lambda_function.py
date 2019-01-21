import boto3
import uuid
import csv

# Créer les clients boto3 pour avoir une connexion vers les APIs AWS
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

def lambda_handler(event, context):
    print("Start function")
    # pour chaque evenement (ici fichier uploadé sur S3)
    for record in event['Records']:
        # Récupérer le nom du bucket et le nom du fichier
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'] 
        print("Bucket name:" + bucket)
        print("Key name:" + key)
        # Construire le path pour le fichier telechargé
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
        # Télécharger le fichier
        s3_client.download_file(bucket, key, download_path)
        print("File is downloaded to the path" + download_path)
        # Lire le fichier et recupérer le contenu
        f= open(download_path,"r") 
        content_file = f.read()
        # Afficher le contenu du fichier
        print(content_file)
        # Si le mot "Booked" apparait dans le fichier, envoyer une notification
        if ("booked") in content_file:
            sns_client.publish(TopicArn='arn:aws:sns:eu-west-1:xxxxxxxx:topic-lambda-file-processing',
                               Message='You successfuly booked your trip')
            print("Notifications are successfuly sent to the sns topic subscriptions")
        f.close()
        # un second exemple
        with open(download_path) as f:
            content = f.readlines()
            for line in content:
                if "prix" in line:
                    sns_client.publish(TopicArn='arn:aws:sns:eu-west-1:xxxxxxxxx:topic-lambda-file-processing',
                               Message='The price of your booking is: '+line )
    return True
