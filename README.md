# Eleições para Prefeitura de São Paulo no Twitter

Este repositório contém as ferramentas usadas para obter e analisar os tweets relacionados à
eleição para a prefeitura de São Paulo de 2020.

## `Dockerfile`

Contém os comandos para compilar e executar os serviços necessários.

### Instruções de uso

    docker build -t gcr.io/prefs-2020/fetch-tweets .
    docker push gcr.io/prefs-2020/fetch-tweets
    gcloud ...

## `fetch/`

Conecta com o Twitter para baixar em tempo real os tweets relacionados ao termos selecionados, e
armazena os tweets individuais em formato JSON *sem processamento* em um bucket do Google Cloud Storage.

### Instruções de uso

    go build -o bin/fetch fetch/*.go
    bin/fetch -keywords "#FocoForcaFe,bruno covas" -languages "pt" -follow "brunocovas"

## `filter/`

Lê os objetos presentes em uma pasta específica do bucket (e.g., `tweets/dt=2020-10-18`), limpa
alguns de seus campos que causam problemas ao importar no BigQuery, e então escreve em um único arquivo
JSONL (JSON delimitado por '\n').

A importação no BigQuery por enquanto é manual. Esse script não precisa ser executado na nuvem, pelo
menos não para o volume diário atual de tweets (~20.000 tweets/dia são lidos e reescritos em ~3min).

### Instruções de uso

    go build -o bin/filter filter/*.go
    bin/filter -ingest-date 2020-10-27
