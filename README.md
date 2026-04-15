# Exportador de dados de poder

Script em Python para extrair os dados (JSON) da URL da promoção e exportar para CSV.

Arquivos criados
- [export_power.py](export_power.py)
- [requirements.txt](requirements.txt)

Como usar

1. Instale dependências:

```powershell
pip install -r requirements.txt
```

2. Execute o script (usa a URL padrão da solicitação curl fornecida):

```powershell
python export_power.py
```

Opções úteis:

- `--url` : URL para buscar os dados (padrão já configurado)
- `--output` ou `-o` : arquivo CSV de saída (padrão `power_export.csv`)
- `--raw` : salva a resposta JSON bruta (padrão `raw_response.json`)
- `--fields` : campos específicos separados por vírgula para exportar

Exemplo de uso com saída customizada:

```powershell
python export_power.py --output usuarios.csv --raw resp.json
```

Observações

- O script tenta localizar a primeira lista encontrada no JSON de resposta e
  achatar objetos aninhados para colunas no CSV.
- Cada linha terá a coluna `extraction_date` com a data/hora UTC da extração.

Site

- O site estático para o projeto está em `docs/`. Para publicar no GitHub Pages, nas configurações do repositório escolha a pasta `docs/` como fonte de Pages.
- Página local: [docs/index.html](docs/index.html)
