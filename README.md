# Demonstração de mensageria com RabbitMQ

Exemplo de desacoplamento entre produtor e consumidor usando RabbitMQ, com
acknowledgement manual, redelivery por NACK e Dead Letter Queue (DLQ).

## Topologia

```text
notificacoes.exchange (direct)
        |
        | routing key: notificacoes
        v
notificacoes (quorum, x-delivery-limit=3)
        |
        | após 3 falhas / NACK final
        v
notificacoes.dlx (direct)
        |
        | routing key: notificacoes.dlq
        v
notificacoes.dlq (quorum)
```

A fila principal é uma **quorum queue** porque o argumento `x-delivery-limit`
só é aplicado automaticamente nesse tipo de fila. O consumer acompanha o número
da entrega e, na terceira falha, executa `basic_nack` com `requeue=False` para
encaminhar a mensagem ao DLX/DLQ. O `x-delivery-limit=3` permanece configurado
como proteção adicional do broker para mensagens devolvidas por falhas de
consumidor.

## Pré-requisitos

- Docker e Docker Compose
- Python 3.11 ou superior

## 1. Subir o RabbitMQ

```bash
docker compose up -d
```

Aguarde o container ficar saudável:

```bash
docker compose ps
```

Painel de management: <http://localhost:15672>

- Usuário: `app`
- Senha: `app`

## 2. Instalar as dependências

No PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

No Linux/macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3. Executar a demonstração

Abra três terminais com o ambiente virtual ativado.

**Terminal 1 — consumidor principal:**

```bash
python consumer.py
```

**Terminal 2 — produtor:**

```bash
python producer.py --demo
```

O modo `--demo` publica quatro mensagens:

| id_pedido | cliente | valor  | Resultado |
|-----------|---------|--------|-----------|
| 123       | Ana     | 99.90  | ACK       |
| 124       | Bruno   | 150.00 | NACK (valor inteiro par) |
| 125       | Carla   | -10.00 | NACK (valor negativo) |
| 126       | Diego   | 75.50  | ACK       |

Para publicar uma mensagem específica:

```bash
python producer.py --id-pedido 200 --cliente "Maria" --valor 99.90
```

**Terminal 3 — consumidor da DLQ:**

```bash
python dlq_consumer.py
```

As mensagens 124 e 125 serão entregues três vezes no total, conforme o log do
consumidor principal, e depois aparecerão na DLQ com o cabeçalho `x-death`.

## Consumidores concorrentes na fila principal

Várias instâncias do mesmo consumer podem ler da fila `notificacoes`. Com
`prefetch_count=1`, o RabbitMQ entrega cada mensagem a apenas um consumidor e
alterna a distribuição entre os consumidores disponíveis.

Encerre o consumer anterior e abra dois terminais:

```bash
# Terminal 1
python consumer.py --name consumer-1

# Terminal 2
python consumer.py --name consumer-2
```

Em um terceiro terminal, publique mensagens:

```bash
python producer.py --demo
```

Cada linha de log identifica a instância que processou o pedido:

```text
[consumer.consumer-1] [ACK] id_pedido=123 ...
[consumer.consumer-2] [ACK] id_pedido=126 ...
```

No painel de management, a fila `notificacoes` deve mostrar dois consumidores.
A divisão é aproximadamente round-robin; a ordem e a quantidade exata podem
variar conforme o tempo de processamento e o momento em que cada instância se
conectou.

## Publish/Subscribe com fanout

Neste padrão, o produtor publica em um exchange `fanout` e cada consumidor
declara sua própria fila temporária. Todas as filas vinculadas recebem uma
cópia de cada evento.

```text
producer_fanout.py
        |
        v
pedidos.fanout (fanout)
        |-------------------------|
        v                         v
fila temporária              fila temporária
[Notificações]                    [Analytics]
```

Abra dois terminais e inicie os consumidores:

```bash
# Terminal 1
python consumer_notificacoes.py

# Terminal 2
python consumer_analytics.py
```

Em um terceiro terminal, publique os eventos:

```bash
python producer_fanout.py --demo
```

O modo `--demo` publica três eventos com intervalo de um segundo. Ambos os
consumidores devem exibir os mesmos `id_pedido`:

```text
[Notificações] Processando id_pedido=201 cliente=Ana valor=99.90
[Analytics] Processando id_pedido=201 cliente=Ana valor=99.90
```

Também é possível publicar um evento específico:

```bash
python producer_fanout.py --id-pedido 300 --cliente "Maria" --valor 99.90
```

As filas fanout são exclusivas, temporárias e têm nomes como
`amq.gen-...`. Elas são removidas quando o consumer encerra. Por isso, inicie os
consumidores antes do produtor; eventos publicados sem filas vinculadas são
descartados.

## Como a falha é simulada

O consumer rejeita mensagens cujo valor seja negativo ou um número inteiro par:

```python
def failure_reason(valor: Decimal) -> str | None:
    if valor < 0:
        return "valor negativo"
    if valor == valor.to_integral_value() and int(valor) % 2 == 0:
        return "valor inteiro par"
    return None
```

Nas duas primeiras falhas, o consumer usa:

```python
channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
```

Isso devolve a mensagem à fila e demonstra o redelivery. Na terceira falha, usa
`basic_nack(requeue=False)`, fazendo a mensagem seguir para o DLX e ser roteada
para `notificacoes.dlq`.

O número da tentativa é lido do header `x-acquired-count` (RabbitMQ 4.3+) ou de
`x-delivery-count` (versões anteriores). Essa distinção é necessária porque, a
partir do RabbitMQ 4.3, `basic.nack` com `requeue=True` não incrementa mais o
`delivery count` usado pelo `x-delivery-limit`.

Em caso de sucesso, o consumer usa `basic_ack`. O `auto_ack=False` garante que
a mensagem não seja removida antes do processamento terminar.

## Configuração opcional

As conexões podem ser ajustadas pelas variáveis de ambiente:

| Variável | Padrão |
|----------|--------|
| `RABBITMQ_HOST` | `localhost` |
| `RABBITMQ_PORT` | `5672` |
| `RABBITMQ_USER` | `app` |
| `RABBITMQ_PASSWORD` | `app` |
| `RABBITMQ_VHOST` | `/` |
| `MAX_DELIVERIES` | `3` |
| `RETRY_DELAY_SECONDS` | `1` |

Se alterar `MAX_DELIVERIES` depois que a fila já foi criada, será necessário
recriar a fila, pois os argumentos de uma fila são imutáveis. Para limpar todo o
ambiente e o volume de dados:

```bash
docker compose down -v
```

## Limitações do exemplo

- O processamento é idempotente apenas para esta demonstração. Em produção,
  consumidores devem tratar redelivery sem duplicar efeitos colaterais.
- Publisher confirms não foram incluídos para manter o foco em ACK/NACK e DLQ.
- O `time.sleep` antes do NACK facilita a leitura dos logs e não é necessário
  em um consumidor real.
