# DAG-csv-to-mongodb-fuel-prices

1. O Intuito da DAG é ler os dados do arquivo csv obtido portal dados.gov.br "Etanol + Gasolina Comum - Fevereiro/2025".
2. Agrupar os dados através das colunas 'Estado - Sigla', 'Produto', 'Unidade de Medida'.
3. Gerar as seguintes colunas: 'Menor_Valor', 'Maior_Valor' e 'Diferenca'.
4. Contruir um dataframe com o resultado da análise.
5. Persistir o resultado em um banco relacional, "MongoDB".
