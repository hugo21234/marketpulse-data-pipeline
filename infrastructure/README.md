# Inicialização econômica do Airflow

Os arquivos desta pasta implementam o início diário da EC2 sem access keys permanentes.

## Provisionar na AWS

Use uma identidade administrativa temporária ou federada na conta `966725470611`; não use a role da EC2 para criar os recursos.

```bash
cd /home/ubuntu/marketpulse
zip -j /tmp/marketpulse-start-ec2.zip infrastructure/lambda/lambda_function.py
zip -j /tmp/marketpulse-stop-ec2.zip infrastructure/lambda/stop_lambda_function.py

aws iam create-role --role-name marketpulse-start-ec2-role --assume-role-policy-document file://infrastructure/iam/marketpulse-start-ec2-role-trust-policy.json
aws iam put-role-policy --role-name marketpulse-start-ec2-role --policy-name marketpulse-start-ec2-policy --policy-document file://infrastructure/iam/marketpulse-start-ec2-role-policy.json
aws iam create-role --role-name marketpulse-start-airflow-scheduler-role --assume-role-policy-document file://infrastructure/iam/marketpulse-start-airflow-scheduler-role-trust-policy.json
aws iam put-role-policy --role-name marketpulse-start-airflow-scheduler-role --policy-name marketpulse-start-airflow-scheduler-policy --policy-document file://infrastructure/iam/marketpulse-start-airflow-scheduler-role-policy.json
aws iam create-role --role-name marketpulse-stop-ec2-role --assume-role-policy-document file://infrastructure/iam/marketpulse-stop-ec2-role-trust-policy.json
aws iam put-role-policy --role-name marketpulse-stop-ec2-role --policy-name marketpulse-stop-ec2-policy --policy-document file://infrastructure/iam/marketpulse-stop-ec2-role-policy.json
aws iam create-role --role-name marketpulse-stop-airflow-scheduler-role --assume-role-policy-document file://infrastructure/iam/marketpulse-stop-airflow-scheduler-role-trust-policy.json
aws iam put-role-policy --role-name marketpulse-stop-airflow-scheduler-role --policy-name marketpulse-stop-airflow-scheduler-policy --policy-document file://infrastructure/iam/marketpulse-stop-airflow-scheduler-role-policy.json

aws lambda create-function --function-name marketpulse-start-ec2 --runtime python3.13 --handler lambda_function.lambda_handler --timeout 15 --memory-size 128 --role arn:aws:iam::966725470611:role/marketpulse-start-ec2-role --zip-file fileb:///tmp/marketpulse-start-ec2.zip --environment file://infrastructure/lambda/environment.json --region sa-east-1
aws lambda create-function --function-name marketpulse-stop-ec2 --runtime python3.13 --handler stop_lambda_function.lambda_handler --timeout 15 --memory-size 128 --role arn:aws:iam::966725470611:role/marketpulse-stop-ec2-role --zip-file fileb:///tmp/marketpulse-stop-ec2.zip --environment file://infrastructure/lambda/stop_environment.json --region sa-east-1
aws scheduler create-schedule --cli-input-json file://infrastructure/scheduler/marketpulse-start-airflow-ec2.json --region sa-east-1
aws scheduler create-schedule --cli-input-json file://infrastructure/scheduler/marketpulse-stop-airflow-ec2.json --region sa-east-1
```

O Scheduler é criado como `DISABLED`. Depois do teste manual, habilite-o no Console em **EventBridge Scheduler → marketpulse-start-airflow-ec2 → Enable**.

## Instalar o serviço da EC2

Execute na instância EC2:

```bash
sudo install -m 0644 infrastructure/systemd/marketpulse-airflow.service /etc/systemd/system/marketpulse-airflow.service
sudo systemctl daemon-reload
sudo systemctl enable marketpulse-airflow.service
sudo systemctl start marketpulse-airflow.service
sudo systemctl status marketpulse-airflow.service
docker compose -f /home/ubuntu/marketpulse/airflow/docker-compose.yaml ps
```

## Teste e diagnóstico

1. No Console Lambda, abra `marketpulse-start-ec2`, escolha **Test**, crie um evento `{}` e execute-o com a EC2 parada.
2. Confirme a resposta `start_requested`; uma segunda chamada durante `pending` ou `running` não deve iniciar novamente.
3. Após validar a DAG, teste `marketpulse-stop-ec2` com `{}` e confirme a resposta `stop_requested` e o estado `stopped`; ela nunca termina a instância.
4. Verifique `/aws/lambda/marketpulse-start-ec2` e `/aws/lambda/marketpulse-stop-ec2` no CloudWatch, o estado da EC2 e o histórico dos dois Schedulers.
5. Na EC2, use `journalctl -u marketpulse-airflow.service -b`, `docker compose ps` e `docker compose logs` dentro de `/home/ubuntu/marketpulse/airflow`.

O stop ocorre diariamente às 00:15 BRT, deixando 15 minutos de margem para a DAG das 00:00. `AccessDenied` indica policy ou ARN incorreto; `InvalidInstanceID.NotFound` indica ID ou região incorretos.

