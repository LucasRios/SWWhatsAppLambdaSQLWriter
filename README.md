# SW WhatsApp Inbound Writer (Multi-Tenant)

Este componente é responsável pela **persistência final** das mensagens recebidas no banco de dados do cliente correspondente.

## 🧠 Inteligência de Roteamento

Diferente de sistemas legados, esta Lambda não possui um banco de dados fixo. Ela utiliza um modelo **Multi-Tenant** via banco Master:
1. **Identificação**: Analisa o JSON em busca de IDs da Meta ou Whapi.
2. **Lookup**: Consulta o banco Master para descobrir em qual banco de dados (tenant) aquele ID está registrado.
3. **Cache**: Armazena o mapeamento em memória (`ConcurrentDictionary`) para acelerar processamentos futuros.
4. **Escrita**: Conecta-se ao banco específico do cliente e salva o log da mensagem.

## 🛠️ Detalhes Técnicos

- **Concurrent Cache**: Implementado para reduzir a carga no banco Master e diminuir a latência.
- **Dynamic Connection**: A string de conexão é montada em tempo de execução.
- **Retry Mechanism**: Caso o banco do cliente esteja offline, a exceção relançada faz com que o SQS mantenha a mensagem para tentativas posteriores.

## 📊 Fluxo de Dados

`SQS (Receiver)` -> `Inbound Writer` -> `Lookup Master DB` -> `Insert Client DB`

## ⚙️ Configuração

- `MASTER_CONN_STRING`: String de conexão com o banco de roteamento central.
- A tabela Master deve conter as colunas mapeadas no código: `WhapiChannelID` e `MetaIdWppBusiness`.
