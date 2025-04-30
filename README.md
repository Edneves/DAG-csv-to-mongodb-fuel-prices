# DAG-csv-to-mongodb-fuel-prices

-> O Intuito da DAG é ler os dados do arquivo csv obtido portal dados.gov.br "Etanol + Gasolina Comum - Fevereiro/2025".
-> Agrupar os dados através das colunas 'Estado - Sigla', 'Produto', 'Unidade de Medida'.
-> Gerar as seguintes colunas: 'Menor_Valor', 'Maior_Valor' e 'Diferenca'.
-> Contruir um dataframe com o resultado da análise.
-> Persistir o resultado em um banco relacional, "MongoDB".
