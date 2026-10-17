1. Faça o build e suba os containers (o banco e o airflow vai executar automaticamente)

```bash
docker-compose up -d --build
```

2. Aguarde alguns segundos...

3. Execute o comando para ver a senha de admin

```bash
docker-compose logs app | head -n 30 | grep -e "admin"
```

4. Abra o navegador e navegue até `http://localhost:8080`

5. Faça login com `admin` e a senha da etapa anterior

6. Vá até `DAGS` no menu esquerdo e digite `api_convenios_ceara_postgresql` para encontrar a DAG de convenios
