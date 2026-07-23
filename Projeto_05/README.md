# Projeto 05 - Sales Analytics Pipeline

Pipeline de Engenharia de Dados desenvolvido com PySpark.

## Arquitetura

Raw (CSV)
↓
Bronze (Parquet)
↓
Silver (Parquet)
↓
Gold (Em desenvolvimento)

## Tecnologias

- Python 3.12
- PySpark
- Parquet
- Docker (em desenvolvimento)

## Estrutura

Projeto_05/
├── data/
├── src/
├── requirements.txt
└── README.md

## Como executar

1. Criar ambiente virtual
2. Instalar dependências

```bash
pip install -r requirements.txt
```

3. Executar

```bash
python src/main.py
```