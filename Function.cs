using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using System.Text.Json.Nodes;
using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using System.Data;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace WhatsAppInboundWriter
{
    public class SqsProcessor
    {
        // Cache estático para evitar consultas repetitivas ao banco Master (Otimização de custo e tempo)
        private static readonly ConcurrentDictionary<string, string> _cacheConexoes = new ConcurrentDictionary<string, string>();

        // String de conexão com o banco que contém o mapeamento de IDs vs Clientes
        private const string MASTER_CONN_STRING = "<MASTER_CONN_STRING>";

        public async Task FunctionHandler(SQSEvent evnt, ILambdaContext context)
        {
            if (evnt?.Records == null) return;

            foreach (var message in evnt.Records)
            {
                try
                {
                    await ProcessMessageAsync(message, context);
                }
                catch (Exception ex)
                {
                    // Se falhar a gravação, a mensagem volta para o SQS para nova tentativa
                    context.Logger.LogLine($"ERRO CRÍTICO NO WRITER (MessageId: {message.MessageId}): {ex.Message}");
                    throw;
                }
            }
        }

        /// <summary>
        /// Processa a mensagem, identifica o banco de destino e realiza o INSERT
        /// </summary>
        private async Task ProcessMessageAsync(SQSEvent.SQSMessage message, ILambdaContext context)
        {
            string jsonPayload = message.Body;
            if (string.IsNullOrWhiteSpace(jsonPayload)) return;

            var jsonNode = JsonNode.Parse(jsonPayload);

            // Código identificador da origem para o banco (2: Whapi, 3: Meta)
            string CodsysBotJsonComandos = "2";

            // Extração de IDs para roteamento
            string whapiChannelId = jsonNode?["channel_id"]?.ToString();
            string metaBusinessId = jsonNode?["entry"]?[0]?["id"]?.ToString();

            string dbNome = null;

            // Lógica de Desvio Multi-Tenant
            if (!string.IsNullOrEmpty(metaBusinessId))
            {
                dbNome = await GetDatabaseByMetaId(metaBusinessId, context);
                CodsysBotJsonComandos = "3";
            }
            else if (!string.IsNullOrEmpty(whapiChannelId))
            {
                dbNome = await GetDatabaseByWhapiId(whapiChannelId, context);
                CodsysBotJsonComandos = "2";
            }

            if (string.IsNullOrEmpty(dbNome))
            {
                context.Logger.LogLine($"Aviso: Mapeamento ausente para MetaId: {metaBusinessId} ou WhapiId: {whapiChannelId}");
                return;
            }

            // Monta a Connection String para o banco do cliente específico
            // Nota: dbNome é injetado dinamicamente
            string connStringCliente = $"Server=...;Database={dbNome};User Id=...;Password=...";

            using (var connection = new SqlConnection(connStringCliente))
            {
                await connection.OpenAsync();

                // Chama a lógica de inserção no banco do cliente
                var sql = "INSERT INTO ....";

                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.Add("@json", SqlDbType.NVarChar, -1).Value = jsonPayload;
                    command.Parameters.Add("@CodsysBotJsonComandos", SqlDbType.Int).Value = CodsysBotJsonComandos;

                    await command.ExecuteNonQueryAsync();
                    context.Logger.LogLine($"Sucesso: JSON gravado no banco {dbNome}.");
                }
            }
        }

        // Métodos de auxílio para busca e cache de banco
        private async Task<string> GetDatabaseByWhapiId(string id, ILambdaContext context)
        {
            return await GetDatabaseFromMaster("WhapiChannelID", id, "WHAPI_" + id, context);
        }

        private async Task<string> GetDatabaseByMetaId(string id, ILambdaContext context)
        {
            return await GetDatabaseFromMaster("MetaIdWppBusiness", id, "META_" + id, context);
        }

        /// <summary>
        /// Consulta o banco Master para encontrar o banco de dados do cliente
        /// </summary>
        private async Task<string> GetDatabaseFromMaster(string column, string id, string cacheKey, ILambdaContext context)
        {
            // Tenta obter do cache em memória primeiro
            if (_cacheConexoes.TryGetValue(cacheKey, out string dbNome)) return dbNome;

            try
            {
                using (var connection = new SqlConnection(MASTER_CONN_STRING))
                {
                    await connection.OpenAsync();

                    // Busca dinâmica baseada na coluna (Meta ou Whapi)
                    var query = $"SELECT DatabaseName.... WHERE {column} = @id";

                    using (var command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@id", id);
                        var result = await command.ExecuteScalarAsync();

                        if (result != null && result != DBNull.Value)
                        {
                            string db = result.ToString();
                            _cacheConexoes.TryAdd(cacheKey, db); // Guarda no cache
                            return db;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                context.Logger.LogLine($"Erro ao consultar Master ({column}): {ex.Message}");
                throw;
            }
            return null;
        }
    }
}