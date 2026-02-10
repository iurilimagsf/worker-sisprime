# Messaging Standalone - Aplicação Independente de Mensageria SIFEN

Aplicação standalone para processamento de faturas eletrônicas do Paraguai (**SIFEN** - Sistema de Facturación Electrónica Nacional) via mensageria RabbitMQ, com conexão direta ao Oracle — sem dependências do Django.

## Aviso Legal e Isenção de Responsabilidade

**IMPORTANTE:** Este código é fornecido "como está" (as-is) para fins de referência e desenvolvimento independente.

A GSF (GSF Soluções) não possui qualquer ligação, responsabilidade ou obrigação legal relacionada a:

- Qualquer desenvolvimento, modificação ou implementação realizada com base neste código
- Problemas, erros ou falhas que possam ocorrer durante o uso desta aplicação
- Consequências decorrentes do uso ou da não utilização deste software
- Compatibilidade com sistemas, ambientes ou requisitos específicos
- Suporte técnico, manutenção ou atualizações futuras
- Conformidade com regulamentações, normas ou especificações técnicas

O uso deste código é de inteira responsabilidade do desenvolvedor ou organização que o implementar. Recomenda-se realizar testes adequados, validações de segurança e revisões de código antes de utilizar em ambiente de produção.

A GSF não se responsabiliza por:
- Perdas de dados ou interrupções de serviço
- Não conformidade com requisitos legais ou regulatórios
- Problemas de segurança ou vulnerabilidades
- Incompatibilidades com outros sistemas ou componentes

Ao utilizar este código, você reconhece e concorda que assume total responsabilidade por sua implementação e uso.

---

## Índice

- [Estrutura do Projeto](#estrutura-do-projeto)
- [Pré-requisitos](#pré-requisitos)
- [Início Rápido com Docker (Recomendado)](#início-rápido-com-docker-recomendado)
- [Instalação Local (Sem Docker)](#instalação-local-sem-docker)
- [Configuração](#configuração)
- [Uso](#uso)
- [Arquitetura](#arquitetura)
- [Fluxos de Processamento](#fluxos-de-processamento)
- [Estrutura de Mensagens RabbitMQ](#estrutura-de-mensagens-rabbitmq)
- [Códigos de Status](#códigos-de-status)
- [Logs](#logs)
- [Troubleshooting](#troubleshooting)
- [Desenvolvimento](#desenvolvimento)
- [Licença e Uso](#licença-e-uso)

---

## Estrutura do Projeto

```
messaging_standalone/
├── .dockerignore            # Arquivos ignorados no build Docker
├── .env.example             # Template de variáveis de ambiente
├── Dockerfile               # Imagem Docker da aplicação
├── docker-compose.yml       # Orquestração Docker (RabbitMQ + Worker)
├── requirements.txt         # Dependências Python
├── README.md                # Este arquivo
├── __init__.py              # Inicialização do módulo (v1.0.0)
├── config.py                # Configurações, constantes e validação
├── database.py              # Acesso direto ao Oracle (oracledb thin mode)
├── sifen_xml.py             # Manipulação XML, assinatura digital e QR Code
├── sifen_api.py             # Comunicação HTTP/SOAP com os WebServices SIFEN
├── handlers.py              # Handlers de processamento (enviar, consultar, cancelar)
├── publisher.py             # Publicação de mensagens no RabbitMQ
└── worker.py                # Worker principal (consumidor RabbitMQ)
```

---

## Pré-requisitos

### Com Docker (Recomendado)

- [Docker](https://docs.docker.com/get-docker/) >= 20.10
- [Docker Compose](https://docs.docker.com/compose/install/) >= 2.0
- Banco de dados Oracle acessível na rede

### Sem Docker (Local)

- Python >= 3.10
- RabbitMQ >= 3.x rodando
- Banco de dados Oracle acessível na rede

---

## Início Rápido com Docker (Recomendado)

O Docker Compose sobe automaticamente o **RabbitMQ** e o **Worker**, pronto para uso:

```bash
# 1. Copie o template de variáveis e configure com seus dados
cp .env.example .env

# 2. Edite o .env com suas credenciais Oracle e URLs do SIFEN
nano .env   # ou seu editor preferido

# 3. Suba tudo (RabbitMQ + Worker)
docker compose up -d

# 4. Acompanhe os logs do worker
docker compose logs -f worker

# 5. (Opcional) Acesse o painel do RabbitMQ
#    http://localhost:15672
#    Usuário/senha: conforme definido no .env (RABBITMQ_USER/RABBITMQ_PASS)
```

### Comandos Docker úteis

```bash
# Parar tudo
docker compose down

# Reconstruir imagem após mudanças no código
docker compose up -d --build

# Escalar para múltiplos workers
docker compose up -d --scale worker=3

# Ver status dos serviços
docker compose ps

# Ver logs de todos os serviços
docker compose logs -f

# Reiniciar apenas o worker
docker compose restart worker
```

### Nota sobre Oracle no Docker

O banco Oracle é **externo** ao Docker Compose (não é gerenciado por ele). Configure o `ORACLE_CONNECTION_STRING` no `.env` para apontar para seu Oracle:

| Situação | Host a usar |
|---|---|
| Oracle na mesma máquina (Linux) | IP da interface `docker0` (ex: `172.17.0.1`) |
| Oracle na mesma máquina (Mac/Windows) | `host.docker.internal` |
| Oracle em outro servidor | IP ou hostname do servidor |

Exemplo:
```bash
ORACLE_CONNECTION_STRING=meu_user/minha_senha@172.17.0.1:1521/meu_service
```

### Sobre o Modo Thin (oracledb)

A aplicação utiliza **python-oracledb** em **thin mode** por padrão, o que significa:

- **Sem necessidade de Oracle Instant Client** — nenhuma biblioteca nativa é necessária
- Imagem Docker leve (~150MB)
- Funciona para a maioria dos cenários de conexão Oracle

Se precisar de funcionalidades avançadas que requerem **thick mode** (Oracle Client), há instruções comentadas no `Dockerfile` e no `database.py` para ativar.

---

## Instalação Local (Sem Docker)

```bash
# 1. Instale as dependências
pip install -r requirements.txt

# 2. Copie e configure o .env
cp .env.example .env
nano .env

# 3. Ajuste o RABBITMQ_HOST para localhost (se o RabbitMQ roda localmente)
#    RABBITMQ_HOST=localhost

# 4. Inicie o worker
python -m messaging_standalone.worker
```

---

## Configuração

Toda a configuração é feita via **variáveis de ambiente**. A aplicação suporta:

1. **Docker Compose** — variáveis injetadas via `env_file` (arquivo `.env`)
2. **Arquivo `.env`** — carregado automaticamente via `python-dotenv`
3. **Variáveis de ambiente do sistema** — têm precedência sobre o `.env`

### Template Completo (`.env.example`)

```bash
# ==== BANCO DE DADOS ORACLE ====
# Opção 1: String completa (formato: user/password@host:port/service_name)
ORACLE_CONNECTION_STRING=user/password@host:1521/service_name

# Opção 2: Componentes separados (usado se ORACLE_CONNECTION_STRING não estiver definido)
# ORACLE_USER=usuario
# ORACLE_PASSWORD=senha
# ORACLE_DSN=host:1521/service_name

# ==== RABBITMQ ====
RABBITMQ_HOST=rabbitmq          # Use "rabbitmq" para Docker, "localhost" para local
RABBITMQ_PORT=5672
RABBITMQ_USER=sifen_user
RABBITMQ_PASS=sifen_pass_segura

# Portas expostas no host (apenas docker-compose)
RABBITMQ_EXTERNAL_PORT=5672
RABBITMQ_MGMT_PORT=15672

# ==== SIFEN - URLs dos WebServices ====
URL_SIFEN_CONSULTA_LOTE=https://ekuatia.set.gov.py/sifen/ws/async/consulta-lote-v150.wsdl
URL_SIFEN_RECEBE_LOTE=https://ekuatia.set.gov.py/sifen/ws/async/recibe-de-v150.wsdl
URL_SIFEN_QR=https://ekuatia.set.gov.py/consultas/qr?
URL_SIFEN_EVENTO=https://ekuatia.set.gov.py/sifen/ws/eventos/evento-v150.wsdl

# ==== WORKER ====
DELAY_TTL_MS=30000              # Delay antes de consultar status (ms), padrão: 30s
```

### Detalhamento das Variáveis

#### Oracle (obrigatório — uma das opções)

| Variável | Descrição | Exemplo |
|---|---|---|
| `ORACLE_CONNECTION_STRING` | String de conexão completa | `user/pass@host:1521/service` |
| `ORACLE_USER` | Usuário Oracle | `meu_usuario` |
| `ORACLE_PASSWORD` | Senha Oracle | `minha_senha` |
| `ORACLE_DSN` | DSN (host:porta/service) | `192.168.1.10:1521/ORCL` |

#### RabbitMQ (obrigatório)

| Variável | Descrição | Padrão |
|---|---|---|
| `RABBITMQ_HOST` | Host do RabbitMQ | `localhost` |
| `RABBITMQ_PORT` | Porta AMQP | `5672` |
| `RABBITMQ_USER` | Usuário | — |
| `RABBITMQ_PASS` | Senha | — |
| `RABBITMQ_VHOST` | Virtual host | `/` |
| `RABBITMQ_EXTERNAL_PORT` | Porta AMQP exposta no host (Docker) | `5672` |
| `RABBITMQ_MGMT_PORT` | Porta do painel web (Docker) | `15672` |

#### SIFEN (obrigatório)

| Variável | Descrição |
|---|---|
| `URL_SIFEN_CONSULTA_LOTE` | URL do WebService de consulta de lote |
| `URL_SIFEN_RECEBE_LOTE` | URL do WebService de recebimento de lote |
| `URL_SIFEN_QR` | URL base para geração do QR Code |
| `URL_SIFEN_EVENTO` | URL do WebService de eventos (cancelamento) |

#### Worker (opcional)

| Variável | Descrição | Padrão |
|---|---|---|
| `DELAY_TTL_MS` | Delay em ms antes da consulta de status | `30000` |

---

## Uso

### Iniciar o Worker

O worker é o processo principal que consome mensagens do RabbitMQ e processa faturas.

**Via Docker (recomendado):**
```bash
docker compose up -d
```

**Localmente:**
```bash
python -m messaging_standalone.worker
```

O worker automaticamente:
1. Valida todas as configurações obrigatórias
2. Conecta ao banco de dados Oracle (thin mode)
3. Conecta ao RabbitMQ
4. Declara e configura filas e exchanges (DLX pattern)
5. Inicia o consumo de mensagens da fila `faturas_para_processar`

### Publicar Mensagens

As funções do módulo `publisher.py` permitem que outras aplicações publiquem mensagens na fila:

#### Enviar Fatura

```python
from messaging_standalone.publisher import processa_fatura

processa_fatura(id_fatura=123)
```

#### Consultar Status

```python
from messaging_standalone.publisher import processa_consulta

processa_consulta(id_fatura=123)
```

#### Cancelar Fatura

```python
from messaging_standalone.publisher import processa_cancelamento

processa_cancelamento(id_fatura=123, motivo="Motivo do cancelamento")
# O motivo é obrigatório e deve ter no mínimo 5 caracteres
```

---

## Arquitetura

### Visão Geral

```
┌─────────────┐     ┌──────────────────┐     ┌────────────────┐     ┌─────────┐
│  Publisher   │────>│    RabbitMQ       │────>│     Worker     │────>│  SIFEN  │
│ (qualquer   │     │                  │     │  (consumidor)  │     │  (SET)  │
│  aplicação) │     │  faturas_para_   │     │                │     │         │
│             │     │  processar       │     │  handlers.py   │     │         │
└─────────────┘     └──────────────────┘     └───────┬────────┘     └─────────┘
                              ▲                      │
                              │                      ▼
                    ┌─────────┴────────┐     ┌────────────────┐
                    │  faturas_wait_   │     │   Oracle DB    │
                    │  30s (delay)     │     │                │
                    │                  │     │ tb_de_emissao  │
                    │  DLX → retry     │     │ tb_de_documento│
                    └──────────────────┘     └────────────────┘
```

### Filas e Exchanges

| Componente | Nome | Descrição |
|---|---|---|
| **Fila principal** | `faturas_para_processar` | Fila onde as mensagens são consumidas e processadas |
| **Fila de delay** | `faturas_wait_30s` | Fila com TTL (30s padrão) para agendar consultas após envio |
| **Exchange DLX** | `faturas_dlx` | Exchange Dead Letter que redireciona mensagens expiradas da fila de delay de volta para a fila principal |
| **Routing Key** | `faturas_routing_key` | Chave de roteamento para o DLX |

### Padrão Dead Letter Exchange (DLX)

O sistema usa o padrão DLX para implementar **delay sem polling**:

1. Após enviar um lote ao SIFEN, a mensagem de consulta é publicada na fila `faturas_wait_30s`
2. A fila tem um TTL de 30 segundos — a mensagem "expira" após esse tempo
3. Ao expirar, o RabbitMQ redireciona automaticamente a mensagem (via DLX) para a fila principal `faturas_para_processar`
4. O worker consome a mensagem de consulta e verifica o status no SIFEN
5. Se ainda estiver processando, repete o ciclo (até 10 tentativas)

### Tabelas Oracle

A aplicação opera sobre duas tabelas:

**`tb_de_emissao`** — Dados da emissão/fatura

| Coluna | Descrição |
|---|---|
| `id` | ID interno auto-incremental |
| `id_docfis` | ID do documento fiscal (chave de busca) |
| `XML` | XML original da fatura |
| `XML_ASSINADO` | XML assinado com QR Code + wrapper rLoteDE |
| `XML_RETORNO` | XML de retorno do SIFEN |
| `XML_CANCELAMENTO_ENVIO` | XML do evento de cancelamento enviado |
| `XML_CANCELAMENTO_RETORNO` | XML de retorno do cancelamento |
| `caminho_certificado` | Caminho do arquivo PFX do certificado digital |
| `senha` | Senha do certificado PFX |
| `id_csc` / `csc` | ID e valor do CSC (Código de Segurança do Contribuinte) |
| `protocolo` | Protocolo retornado pelo SIFEN para consulta de lote |
| `cod_status` / `desc_status` | Código e descrição do status atual |
| `tipo` / `TIPO_DOCTO` | Tipo do documento fiscal |

**`tb_de_documento`** — Status resumido do documento

| Coluna | Descrição |
|---|---|
| `id_doc` | ID do documento (= `id_docfis`) |
| `cod_status` | Código numérico de status |
| `desc_status` | Descrição textual do status |

---

## Fluxos de Processamento

### 1. Envio (ação: `enviar`)

Handler: `handle_enviar()` em `handlers.py`

```
Mensagem recebida → Busca dados no Oracle
    → Assina XML digitalmente (RSA-SHA256)
    → Atualiza dFecFirma com horário atual
    → Gera QR Code (SHA256 com CSC)
    → Insere QR no XML após <Signature>
    → Remove header XML e cria wrapper <rLoteDE>
    → Comprime em ZIP (in-memory) e codifica Base64
    → Cria envelope SOAP e envia para SIFEN
    → Extrai protocolo do retorno
    → Se sucesso: atualiza Oracle + agenda consulta via DLX (30s)
    → Se falha: atualiza Oracle com erro
```

### 2. Consulta (ação: `consultar`)

Handler: `handle_consultar()` em `handlers.py`

```
Mensagem recebida (após delay de 30s) → Busca protocolo no Oracle
    → Cria envelope SOAP de consulta e envia para SIFEN
    → Analisa retorno:
        → "Aprobado": atualiza Oracle com sucesso (cod_status do SIFEN)
        → "Rechazado": atualiza Oracle com rejeição + motivo
        → Erro 0160 (XML Mal Formado): reagenda consulta (erro transitório do SIFEN)
        → Ainda processando: reagenda consulta (tentativa + 1)
        → Excedeu 10 tentativas: atualiza Oracle com cod_status 998
```

### 3. Cancelamento (ação: `cancelar`)

Handler: `handle_cancelar()` em `handlers.py`

```
Mensagem recebida → Busca XML assinado no Oracle
    → Extrai CDC (44 dígitos) do XML via XPath
    → Gera XML de evento de cancelamento (rGesEve)
    → Assina evento digitalmente (Signature como elemento "irmão")
    → Cria envelope SOAP e envia para endpoint de eventos
    → Analisa retorno:
        → Códigos 0500/0501/0600 ou status "Aprobado": cancelamento homologado
        → Outro: erro no cancelamento
    → Atualiza Oracle com resultado
```

---

## Estrutura de Mensagens RabbitMQ

### Envio

```json
{
    "id_fatura": 123
}
```

ou explicitamente:

```json
{
    "id_fatura": 123,
    "acao": "enviar"
}
```

### Consulta

```json
{
    "id_fatura": 123,
    "acao": "consultar",
    "tentativas": 1
}
```

> O campo `tentativas` é incrementado automaticamente a cada reconsulta (máximo: 10).

### Cancelamento

```json
{
    "id_fatura": 123,
    "acao": "cancelar",
    "motivo": "Motivo do cancelamento (mínimo 5 caracteres)"
}
```

> Quando a `acao` não é especificada, o worker assume `"enviar"` como padrão.

---

## Códigos de Status

Códigos internos usados pela aplicação para rastrear o ciclo de vida das faturas:

| Código | Constante | Descrição |
|--------|-----------|-----------|
| `0201` | `CODIGO_STATUS_APROVADO` | Documento aprovado pelo SIFEN |
| `0300` | `CODIGO_STATUS_REJEITADO` | Lote rejeitado pelo SIFEN |
| `0500` | — | Cancelamento aceito (sucesso) |
| `0501` | — | Cancelamento aceito (sucesso) |
| `0600` | — | Cancelamento aceito (sucesso) |
| `600` | `CODIGO_STATUS_CANCELADO` | Nota cancelada com sucesso |
| `900` | `CODIGO_STATUS_ENVIADO` | Lote recebido, aguardando consulta |
| `998` | `CODIGO_STATUS_EXCEDEU_TENTATIVAS` | Excedeu limite de tentativas de consulta |

---

## Logs

A aplicação usa o módulo `logging` padrão do Python com saída para `stdout` — ideal para Docker (`docker compose logs`).

Formato padrão:
```
2026-02-06 14:30:00,123 - messaging_standalone.worker - INFO - [*] Worker aguardando por faturas.
```

Para ativar logs detalhados (debug), altere o nível no `worker.py`:
```python
logging.basicConfig(level=logging.DEBUG)
```

Em Docker, acompanhe os logs em tempo real:
```bash
# Apenas o worker
docker compose logs -f worker

# Todos os serviços
docker compose logs -f

# Últimas 100 linhas
docker compose logs --tail=100 worker
```

---

## Troubleshooting

### Erro de Conexão Oracle

```
Erro ao conectar ao Oracle: DPY-6001: cannot connect to database
```

- Verifique se `ORACLE_CONNECTION_STRING` está no formato correto (`user/pass@host:port/service`)
- Se Oracle está na máquina host, no Docker use `172.17.0.1` (Linux) ou `host.docker.internal` (Mac/Windows) em vez de `localhost`
- Verifique se a porta 1521 está aberta e acessível
- Verifique se o serviço Oracle está ativo

### Erro de Conexão RabbitMQ

```
Não foi possível conectar ao RabbitMQ: [Errno 111] Connection refused
```

- Em Docker: o `docker-compose.yml` já configura `RABBITMQ_HOST=rabbitmq` automaticamente. Verifique se o RabbitMQ está healthy (`docker compose ps`)
- Local: verifique se `RABBITMQ_HOST=localhost` e se o RabbitMQ está rodando
- Verifique credenciais (`RABBITMQ_USER` e `RABBITMQ_PASS`)
- O worker reinicia automaticamente (`restart: unless-stopped`) se o RabbitMQ ainda estiver inicializando

### Worker reiniciando em loop no Docker

- Geralmente acontece quando o Oracle não está acessível. Verifique os logs: `docker compose logs worker`
- Confirme que o host Oracle é alcançável de dentro do container

### Erro 0160 - XML Mal Formado

- Este é um erro transitório do SIFEN. O worker automaticamente reagenda a consulta (até 10 tentativas)
- Se persistir, verifique se o XML original na tabela `tb_de_emissao` está correto

### Erro no SIFEN (Assinatura/Certificado)

- Verifique se o caminho do certificado PFX (`caminho_certificado` na tabela) está acessível pelo worker
- Verifique se a senha do certificado (`senha` na tabela) está correta
- Verifique se o certificado não está expirado
- Em Docker: certifique-se de que o caminho do certificado está montado como volume, se necessário

### Painel RabbitMQ

Acesse `http://localhost:15672` para monitorar filas, mensagens pendentes e conexões. Use as credenciais definidas em `RABBITMQ_USER`/`RABBITMQ_PASS`.

---

## Desenvolvimento

### Dependências

| Pacote | Versão | Uso |
|--------|--------|-----|
| `oracledb` | >= 2.0.0 | Conexão Oracle (thin mode, sem libs nativas) |
| `pika` | 1.3.2 | Cliente RabbitMQ (AMQP) |
| `requests` | 2.32.3 | Requisições HTTP/SOAP para o SIFEN |
| `lxml` | latest | Parser e manipulação de XML |
| `signxml` | 3.2.0 | Assinatura digital XML (RSA-SHA256) |
| `cryptography` | 42.0.5 | Criptografia e conversão PFX → PEM |
| `python-dotenv` | 1.1.1 | Carregamento de variáveis do arquivo `.env` |

### Adicionar Novas Ações

Para adicionar um novo tipo de processamento:

1. **`sifen_xml.py`** — Adicione funções de manipulação XML específicas (se necessário)
2. **`sifen_api.py`** — Adicione funções de comunicação com novos endpoints SIFEN (se necessário)
3. **`handlers.py`** — Crie um novo handler `handle_nova_acao()` seguindo o padrão existente
4. **`handlers.py`** — Registre a nova ação no roteador `on_message_received()`:

```python
# No final de on_message_received(), adicione:
elif acao == 'nova_acao':
    handle_nova_acao(ch, method, emissao, documento, dados)
```

### Thick Mode (Oracle Client)

Se precisar de thick mode para funcionalidades Oracle avançadas:

1. No `Dockerfile`, descomente o bloco "Thick Mode" para instalar o Oracle Instant Client
2. No `database.py`, descomente a linha `oracledb.init_oracle_client()` no método `connect()`
3. Reconstrua a imagem: `docker compose up -d --build`

---

## Licença e Uso

Este código é disponibilizado para uso independente e desenvolvimento. A GSF não mantém responsabilidade sobre modificações, implementações ou uso deste código. Qualquer desenvolvimento realizado sobre esta base é de inteira responsabilidade do desenvolvedor ou organização que o utilizar.

---

**Nota:** Este projeto foi desenvolvido como uma aplicação standalone extraída do sistema GSF para permitir uso independente. A GSF não oferece suporte oficial para esta versão standalone e não se responsabiliza por seu uso ou modificações.
