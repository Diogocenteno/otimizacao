# otimizacao
O código fornecido realiza diversas operações utilizando o PySpark para processar dois DataFrames representando vídeos e comentários. Ele executa operações de limpeza de dados, como renomeação de colunas para evitar inconsistências, realiza vários JOINs utilizando Spark SQL, e otimiza a execução usando particionamento e coalesce.

Pontos principais do código:
Leitura de arquivos Parquet:

Lê os arquivos no formato Parquet (videos-preparados.snappy.parquet e videos-comments-tratados.snappy.parquet) e os carrega em DataFrames Spark.
Limpeza e padronização de colunas:

Renomeia as colunas para evitar espaços e inconsistências, padronizando para snake_case.
Isso facilita a manipulação e evita problemas em operações SQL.
Criação de tabelas temporárias:

Cria tabelas temporárias (video_table e comments_table) para permitir consultas SQL nos DataFrames.
JOIN entre vídeos e comentários:

Realiza um JOIN entre as tabelas temporárias com base no campo video_id.
Inclui etapas para validar os resultados do JOIN com .show().
Otimização com reparticionamento e coalesce:

Usa repartition() para aumentar o paralelismo e otimizar a execução em clusters grandes.
Aplica coalesce() para reduzir o número de partições antes de salvar os resultados.
Filtragem e seleção de colunas relevantes:

Seleciona apenas as colunas necessárias para o processamento final.
Remove registros com valores nulos em video_id.
Armazenamento do resultado:

Salva o DataFrame resultante do JOIN otimizado no formato Parquet.
