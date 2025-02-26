# Projeto de Transformação e Tratamento de Perdas

## Descrição
Este projeto tem como objetivo transformar e tratar os dados de perdas de depósitos e filiais, estruturando as informações para análise e tomada de decisão. Os dados são processados e transformados utilizando **dbt (Data Build Tool)** e, ao final, exportados para o **Amazon Athena**.

## Tecnologias Utilizadas
- **dbt**: Para transformar e modelar os dados de forma eficiente.
- **Amazon Athena**: Para consulta e análise dos dados transformados.
- **SQL**: Para manipulação e modelagem dos dados.

## Fluxo de Processamento
1. **Transformação com dbt**: Modelagem dos dados para padronização e análise.
2. **Exportação para Athena**: Disponibilização dos dados processados para consulta e relatórios.

## Como Executar o Projeto
1. Clone o repositório:
   ```sh
   git clone https://github.com/Marcus-Holanda777/dbt_perdas_resumo.git
   ```
2. Instale as dependências do dbt:
   ```sh
   pip install dbt-core dbt-athena
   ```
3. Configure as credenciais da AWS.
4. Execute o dbt:
   ```sh
   dbt run
   ```
5. Consulte os dados no Athena.

> [!IMPORTANT] 
> Existe um agendamento configurado para rodar o processo diariamente às 9 da manhã via CI/CD.