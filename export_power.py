#!/usr/bin/env python3
"""
Exportador de dados de poder a partir da URL fornecida.

Uso básico:
  python export_power.py

O script faz uma requisição GET na URL padrão (ou passada via --url),
procura por uma lista de objetos no JSON retornado, achata os objetos
e exporta para CSV incluindo a data de extração.
"""
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
import os
import urllib.parse
import re

import requests


def find_first_list(obj: Any) -> Optional[List[Any]]:
    """Procura recursivamente o primeiro valor do tipo lista no JSON."""
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for v in obj.values():
            found = find_first_list(v)
            if found is not None:
                return found
    return None


def flatten(d: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    items: Dict[str, Any] = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items


def rows_from_json_list(lst: List[Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in lst:
        if isinstance(item, dict):
            rows.append(flatten(item))
        else:
            # valores simples (strings/ints) -> colocar como valor na chave 'value'
            rows.append({"value": item})
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta dados de poder para CSV")
    parser.add_argument(
        "--url",
        default="https://vr.igg.com/event/promotion?code=ptb_magnumofspades",
        help="URL para obter os dados (padrão: promoção especificada)",
    )
    parser.add_argument("--output", "-o", default="power_export.csv", help="Arquivo CSV de saída")
    parser.add_argument("--raw", default="raw_response.json", help="Salvar resposta JSON bruta")
    parser.add_argument(
        "--fields",
        help="Lista separada por vírgula de campos a exportar (por exemplo: uid,name,power). Se omitido, exporta todos.",
    )
    parser.add_argument("--timeout", type=int, default=15, help="Timeout da requisição em segundos")
    args = parser.parse_args()

    # obter lista de códigos: --codes (vírgula separada) ou --codes-file (uma por linha)
    parser.add_argument(
        "--codes",
        help="Lista de códigos separados por vírgula. Ex: ptb_magnumofspades,othercode",
    )
    parser.add_argument(
        "--codes-file",
        help="Arquivo com um código por linha para buscar em sequência",
    )

    # reparse args para incluir novos parâmetros
    args = parser.parse_args()

    def get_code_from_url(url: str) -> Optional[str]:
        try:
            p = urllib.parse.urlparse(url)
            qs = urllib.parse.parse_qs(p.query)
            codes = qs.get("code") or qs.get("Code")
            if codes:
                return codes[0]
        except Exception:
            return None
        return None

    # construir lista final de códigos a consultar
    codes_list: List[str] = []
    if args.codes:
        codes_list = [c.strip() for c in args.codes.split(",") if c.strip()]
    elif args.codes_file:
        try:
            with open(args.codes_file, "r", encoding="utf-8") as f:
                codes_list = [line.strip() for line in f if line.strip()]
        except Exception:
            print(f"Não foi possível ler o arquivo de códigos: {args.codes_file}")
            return
    else:
        url_code = get_code_from_url(args.url)
        if url_code:
            codes_list = [url_code]
        else:
            # sem códigos, usar valor literal da URL inteira como fallback único
            codes_list = [""]

    # preparar utilitários e acumular resultados
    def build_url_with_code(base_url: str, code: str) -> str:
        try:
            if not code:
                return base_url
            p = urllib.parse.urlparse(base_url)
            qs = urllib.parse.parse_qs(p.query)
            qs["code"] = [code]
            new_qs = urllib.parse.urlencode(qs, doseq=True)
            return urllib.parse.urlunparse((p.scheme, p.netloc, p.path, p.params, new_qs, p.fragment))
        except Exception:
            sep = "&" if "?" in base_url else "?"
            return f"{base_url}{sep}code={urllib.parse.quote(code)}"

    headers = {
        "User-Agent": "export_power_script/1.0",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }

    master_rows: List[Dict[str, Any]] = []

    # percorrer cada código e acumular resultados
    for code in codes_list:
        url = build_url_with_code(args.url, code)
        raw_name = args.raw
        if code:
            # incluir código no nome do arquivo bruto para não sobrescrever
            raw_name = f"{args.raw.rstrip('.json')}_{code}.json"

        print(f"Requisitando {url}...")
        resp = requests.get(url, headers=headers, timeout=args.timeout)
        resp.raise_for_status()

        # tentar JSON primeiro
        data = None
        try:
            data = resp.json()
        except json.JSONDecodeError:
            # salvar resposta bruta (HTML ou outro)
            with open(raw_name, "w", encoding="utf-8") as f:
                f.write(resp.text)

            # tentar extrair tabela HTML (caso a página retorne HTML com ranking)
            try:
                from bs4 import BeautifulSoup  # type: ignore

                soup = BeautifulSoup(resp.text, "html.parser")
                rows_html = soup.select(".table-body .table-row")
                if rows_html:
                    rows: List[Dict[str, Any]] = []
                    for row in rows_html:
                        tds = [td.get_text(strip=True) for td in row.select(".table-td")]
                        entry: Dict[str, Any] = {}
                        if len(tds) >= 1:
                            entry["rank"] = tds[0]
                        if len(tds) >= 2:
                            entry["chief_id"] = tds[1]
                        if len(tds) >= 3:
                            entry["hall"] = tds[2]
                        if len(tds) >= 4:
                            raw_power = tds[3]
                            num = re.sub(r"[^0-9]", "", raw_power)
                            entry["power"] = int(num) if num else None
                        if code:
                            entry["code"] = code
                        rows.append(entry)

                    extraction_key = "extraction_date"
                    now = datetime.now(timezone.utc).isoformat()
                    for r in rows:
                        r[extraction_key] = now
                        if code:
                            r["code"] = code

                    master_rows.extend(rows)
                    print(f"Encontradas {len(rows)} linhas (HTML) para code={code}.")
                    continue
            except Exception:
                pass

            # fallback regex
            row_pattern = re.compile(r"<div[^>]*class=[\"'](?:(?:\\s|.)*?\btable-row\b(?:\\s|.)*?)[\"'][^>]*>(.*?)</div>\s*</div>", flags=re.IGNORECASE | re.DOTALL)
            td_pattern = re.compile(r"<div[^>]*class=[\"'](?:(?:\\s|.)*?\btable-td\b(?:\\s|.)*?)[\"'][^>]*>(.*?)</div>", flags=re.IGNORECASE | re.DOTALL)

            extracted_rows: List[Dict[str, Any]] = []
            for m in row_pattern.finditer(resp.text):
                row_block = m.group(1)
                tds = td_pattern.findall(row_block)
                tds = [re.sub(r"<[^>]+>", "", td).strip() for td in tds]
                if not tds:
                    continue
                entry: Dict[str, Any] = {}
                if len(tds) >= 1:
                    entry["rank"] = tds[0]
                if len(tds) >= 2:
                    entry["chief_id"] = tds[1]
                if len(tds) >= 3:
                    entry["hall"] = tds[2]
                if len(tds) >= 4:
                    entry["power"] = int(re.sub(r"[^0-9]", "", tds[3])) if re.sub(r"[^0-9]", "", tds[3]) else None
                if code:
                    entry["code"] = code
                extracted_rows.append(entry)

            if not extracted_rows:
                body_match = re.search(r"<div[^>]*class=[\"'](?:(?:\\s|.)*?\btable-body\b(?:\\s|.)*?)[\"'][^>]*>(.*?)</div>\s*</div>", resp.text, flags=re.IGNORECASE | re.DOTALL)
                if body_match:
                    body = body_match.group(1)
                    tds_all = td_pattern.findall(body)
                    tds_all = [re.sub(r"<[^>]+>", "", td).strip() for td in tds_all]
                    for i in range(0, len(tds_all), 4):
                        chunk = tds_all[i:i+4]
                        if not chunk:
                            continue
                        entry = {}
                        if len(chunk) >= 1:
                            entry["rank"] = chunk[0]
                        if len(chunk) >= 2:
                            entry["chief_id"] = chunk[1]
                        if len(chunk) >= 3:
                            entry["hall"] = chunk[2]
                        if len(chunk) >= 4:
                            entry["power"] = int(re.sub(r"[^0-9]", "", chunk[3])) if re.sub(r"[^0-9]", "", chunk[3]) else None
                        if code:
                            entry["code"] = code
                        extracted_rows.append(entry)

            if extracted_rows:
                extraction_key = "extraction_date"
                now = datetime.now(timezone.utc).isoformat()
                for r in extracted_rows:
                    r[extraction_key] = now
                    if code:
                        r["code"] = code
                master_rows.extend(extracted_rows)
                print(f"Encontradas {len(extracted_rows)} linhas (regex) para code={code}.")
                continue

            print(f"Resposta não é JSON e não foi possível extrair tabela HTML para code={code}. Verifique '{raw_name}'.")
            continue

        # se veio JSON, salvar bruto e extrair
        with open(raw_name, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        lst = find_first_list(data)
        if lst is None:
            if isinstance(data, dict):
                lst = [data]
            else:
                print(f"Não foi encontrada uma lista no JSON de resposta para code={code}.")
                continue

        rows = rows_from_json_list(lst)
        if not rows:
            print(f"Lista encontrada mas sem itens para exportar para code={code}.")
            continue

        # anexar extra info e normalizar power
        extraction_key = "extraction_date"
        now = datetime.now(timezone.utc).isoformat()
        for r in rows:
            r[extraction_key] = now
            if code:
                r["code"] = code
            if "power" in r and r["power"] is not None:
                if isinstance(r["power"], str):
                    num = re.sub(r"[^0-9]", "", r["power"]) or None
                    r["power"] = int(num) if num else None

        master_rows.extend(rows)
        print(f"Encontradas {len(rows)} linhas (JSON) para code={code}.")

    # após consultar todos os códigos, escrever CSV acumulado mantendo histórico
    if not master_rows:
        print("Nenhum dado extraído para os códigos fornecidos.")
        return

    # computar cabeçalhos (união de todas as chaves entre histórico e novos)
    all_keys = set()
    for r in master_rows:
        all_keys.update(r.keys())
    extraction_key = "extraction_date"
    all_keys.add(extraction_key)

    existing_rows: List[Dict[str, Any]] = []
    if os.path.exists(args.output):
        try:
            with open(args.output, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                existing_rows = [row for row in reader]
                all_keys.update(reader.fieldnames or [])
            print(f"Carregadas {len(existing_rows)} linhas existentes de {args.output}.")
        except Exception:
            print(f"Falha ao ler {args.output}; continuando sem histórico.")

    # aplicar filtro de campos se informado
    if args.fields:
        wanted = {f.strip() for f in args.fields.split(',') if f.strip()}
        headers_order = [k for k in sorted(all_keys) if (k in wanted) or (k == extraction_key)]
    else:
        headers_order = sorted(all_keys)

    # combinar histórico + novos, removendo duplicados exatos (mesmos valores nas colunas finais)
    combined: List[Dict[str, Any]] = []
    seen = set()

    def row_key(row: Dict[str, Any]) -> tuple:
        return tuple((str(row.get(k, "")).strip() for k in headers_order))

    for row in existing_rows:
        k = row_key(row)
        seen.add(k)
        combined.append(row)

    added = 0
    for row in master_rows:
        # garantir todas as chaves existam
        for k in headers_order:
            if k not in row:
                row[k] = ""
        k = row_key(row)
        if k in seen:
            continue
        seen.add(k)
        combined.append(row)
        added += 1

    # escrever arquivo com histórico preservado (sobrescreve com a nova tabela completa)
    with open(args.output, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers_order, extrasaction="ignore")
        writer.writeheader()
        for r in combined:
            writer.writerow(r)

    print(f"Exportado {len(master_rows)} novas linhas; {added} adicionadas. Total histórico: {len(combined)} linhas em {args.output}.")

    # também gerar cópia para docs/ para uso no site estático (se a pasta existir)
    docs_dir = os.path.join(os.path.dirname(args.output), 'docs')
    try:
        if os.path.isdir(docs_dir):
            docs_full = os.path.join(docs_dir, os.path.basename(args.output))
            with open(docs_full, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=headers_order, extrasaction='ignore')
                writer.writeheader()
                for r in combined:
                    writer.writerow(r)
            print(f"Copiada versão para site: {docs_full}")

            # gerar CSV agregado por data: power_by_date.csv
            agg = {}
            for r in combined:
                # extrair data YYYY-MM-DD
                d = r.get('extraction_date', '')
                if 'T' in d:
                    d = d.split('T', 1)[0]
                if not d:
                    continue
                try:
                    p = int(r.get('power') or 0)
                except Exception:
                    try:
                        p = int(re.sub(r"[^0-9]", "", str(r.get('power') or '')) or 0)
                    except Exception:
                        p = 0
                agg[d] = agg.get(d, 0) + p

            docs_agg = os.path.join(docs_dir, 'power_by_date.csv')
            with open(docs_agg, 'w', encoding='utf-8', newline='') as f:
                w = csv.writer(f)
                w.writerow(['date', 'power_sum'])
                for date_key in sorted(agg.keys()):
                    w.writerow([date_key, agg[date_key]])
            print(f"Gerado agregado para site: {docs_agg}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
