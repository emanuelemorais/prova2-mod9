# Prova 2 - Módulo 9 - Emanuele Lacerda Morais Martins

## Como instalar e rodar o sistema criado

- Entre no diretório `prova2-mod9` e rode o seguinte comando para rodar de forma conteinizada o kafka, zookeeper e o mongo db:

``` 
docker compose up
```

- Entre em `prova2-mod9/consumer` e rode o comando abaixo para iniciar o consumer kafka:

``` 
go run consumer.go
```

- Entre em `prova2-mod9/producer` e rode o comando abaixo para iniciar o producer que irá enviar informações para a fila kafka:

``` 
go run producer.go
```

- Entre em `prova2-mod9/tests` para executar os testes do kafka:

``` 
go test
```

Esse teste irá comparar a mensagem enviada e a recebida para assegurar que não ouve divergencias ao passar pela fila e verifica a persistencia a partir da instalação do mongo db.