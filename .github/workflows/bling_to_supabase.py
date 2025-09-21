#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bling → Supabase (Postgres) ETL diário
- OAuth v3 (refresh_token -> access_token)
- Respeita paginação (limite/pagina)
- Backoff para 429 (rate limit) e refresh em 401
- Upsert em: contatos, vendedores, produtos (derivado do detalhe), pedidos, itens
- Mantém "last_sync" em etl_state para buscar apenas alterados

Requer variáveis de ambiente:
  BLING_CLIENT_ID
  BLING_CLIENT_SECRET
  BLING_REFRESH_TOKEN
  SUPABASE_DB_URL   (ex.: postgresql://postgres.user:PASS@aws-1-sa-east-1.pooler.supabase.com:5432/postgres?sslmode=require)

Dependências:
  pip install requests psycopg2-binary
"""

import os
import time
import json
import sys
import datetime as dt
import requests
import psycopg2
import psycopg2.extras

API_BASE  = "https://api.bling.com.br/Api/v3/"
TOKEN_URL = "https://api.bling.com.br/Api/v3/oauth/token"
UA        = "BlingSupabaseETL/1.1"

CLIENT_ID     = os.environ["BLING_CLIENT_ID"]
CLIENT_SECRET = os.environ["BLING_CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["BLING_REFRESH_TOKEN"]
DB_URL        = os.environ["SUPABASE_DB_URL"]

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})


# ---------- Helpers ----------
def get_access_token():
    """Renova access_token usando refresh_token, com backoff simples."""
    auth = requests.auth._basic_auth_str(CLIENT_ID, CLIENT_SECRET)
    headers = {
        "Authorization": auth,
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "1.0",
        "User-Agent": UA,
    }
    data = {"grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN}

    for delay in (0, 3, 7, 15):  # evita 1015
        if delay:
            time.sleep(delay)
        r = SESSION.post(TOKEN_URL, headers=headers, data=data, timeout=30)
        if r.status_code == 200:
            j = r.json()
            return j["access_token"]
        if r.status_code == 429:
            continue
        try:
            j = r.json()
            err = j.get("error") or j
        except Exception:
            err = r.text
        raise RuntimeError(f"Token error {r.status_code}: {err}")

    raise RuntimeError("Token rate-limited (1015/429). Tente novamente mais tarde.")


def api_get(path, token, params=None, retries=3):
    """GET com Bearer; backoff p/ 429 e refresh em 401."""
    if params is None:
        params = {}
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": UA,
    }
    url = API_BASE + path.lstrip("/")
    r = SESSION.get(url, headers=headers, params=params, timeout=60)

    if r.status_code == 200:
        return r.json()
    if r.status_code == 401 and retries > 0:
        # access_token expirado → renova e repete
        new_tok = get_access_token()
        return api_get(path, new_tok, params, retries - 1)
    if r.status_code == 429 and retries > 0:
        # backoff: 1s, 2s, 4s
        delay = {3: 1, 2: 2, 1: 4}.get(retries, 4)
        time.sleep(delay)
        return api_get(path, token, params, retries - 1)

    try:
        j = r.json()
    except Exception:
        j = r.text
    raise RuntimeError(f"GET {url} -> {r.status_code}: {j}")


def fetch_paged(path, token, params):
    """Itera páginas (limite=100) com 1s entre páginas."""
    limite = int(params.get("limite", 100))
    pagina = 1
    while True:
        p = dict(params)
        p["pagina"] = str(pagina)
        p["limite"] = str(limite)
        j = api_get(path, token, p)
        data = j.get("data", j)
        if isinstance(data, list):
            rows = data
        else:
            rows = [data] if data else []
        if not rows:
            break
        for row in rows:
            yield row
        pagina += 1
        time.sleep(1.0)  # respeita Cloudflare


def coalesce(*vals):
    for v in vals:
        if v is not None:
            return v
    return None


def to_date(s):
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s).date()
    except Exception:
        try:
            return dt.datetime.strptime(s[:10], "%Y-%m-%d").date()
        except Exception:
            return None


# ---------- Upserts ----------
def ensure_dims(conn, pedido_det):
    """Upsert contatos, vendedores e produtos (a partir dos itens do detalhe)."""
    cur = conn.cursor()
    d = pedido_det

    # contato
    contato = d.get("contato")
    if isinstance(contato, dict) and "id" in contato:
        cur.execute(
            """
          insert into contatos (id, nome, tipo_pessoa, numero_documento, updated_at, raw)
          values (%s,%s,%s,%s, now(), %s)
          on conflict (id) do update
          set nome=excluded.nome,
              tipo_pessoa=excluded.tipo_pessoa,
              numero_documento=excluded.numero_documento,
              updated_at=now(),
              raw=excluded.raw
        """,
            (
                int(contato.get("id")),
                contato.get("nome"),
                contato.get("tipoPessoa"),
                contato.get("numeroDocumento"),
                json.dumps(contato, ensure_ascii=False),
            ),
        )

    # vendedor
    vendedor = d.get("vendedor")
    if isinstance(vendedor, dict) and "id" in vendedor:
        cur.execute(
            """
          insert into vendedores (id, nome, updated_at)
          values (%s,%s, now())
          on conflict (id) do update set nome=excluded.nome, updated_at=now()
        """,
            (int(vendedor.get("id")), vendedor.get("nome")),
        )

    # produtos a partir dos itens
    itens = d.get("itens") or []
    for it in itens:
        item = it.get("item", it)
        prod = item.get("produto") or {}
        if not isinstance(prod, dict):
            continue
        pid = prod.get("id")
        try:
            pid = int(pid) if pid is not None else None
        except Exception:
            pid = None
        cur.execute(
            """
          insert into produtos (id, codigo, nome, descricao_curta, preco, preco_custo,
                                estoque, tipo, formato, situacao_nome, situacao_valor,
                                imagem_url, updated_at, raw)
          values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(), %s)
          on conflict (id) do update set
            codigo=excluded.codigo,
            nome=excluded.nome,
            descricao_curta=excluded.descricao_curta,
            preco=excluded.preco,
            preco_custo=excluded.preco_custo,
            estoque=excluded.estoque,
            tipo=excluded.tipo,
            formato=excluded.formato,
            situacao_nome=excluded.situacao_nome,
            situacao_valor=excluded.situacao_valor,
            imagem_url=excluded.imagem_url,
            updated_at=now(),
            raw=excluded.raw
        """,
            (
                pid,
                prod.get("codigo"),
                coalesce(prod.get("descricao"), prod.get("nome")),
                None,
                item.get("valor") or item.get("valorUnitario") or item.get("preco"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                json.dumps(prod, ensure_ascii=False),
            ),
        )
    cur.close()


def upsert_pedido(conn, cab, det):
    cur = conn.cursor()
    situacao = det.get("situacao") or {}
    contato = det.get("contato") or {}
    vendedor = det.get("vendedor") or {}

    cur.execute(
        """
      insert into pedidos (
        id, numero, numero_loja, data, data_saida, data_prevista,
        total_produtos, total, loja_id, situacao_id, situacao_valor,
        contato_id, vendedor_id, updated_at, raw
      )
      values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(), %s)
      on conflict (id) do update set
        numero=excluded.numero,
        numero_loja=excluded.numero_loja,
        data=excluded.data,
        data_saida=excluded.data_saida,
        data_prevista=excluded.data_prevista,
        total_produtos=excluded.total_produtos,
        total=excluded.total,
        loja_id=excluded.loja_id,
        situacao_id=excluded.situacao_id,
        situacao_valor=excluded.situacao_valor,
        contato_id=excluded.contato_id,
        vendedor_id=excluded.vendedor_id,
        updated_at=now(),
        raw=excluded.raw
    """,
        (
            int(det.get("id", cab.get("id"))),
            cab.get("numero"),
            cab.get("numeroLoja"),
            to_date(coalesce(det.get("data"), cab.get("data"))),
            to_date(coalesce(det.get("dataSaida"), cab.get("dataSaida"))),
            to_date(coalesce(det.get("dataPrevista"), cab.get("dataPrevista"))),
            cab.get("totalProdutos"),
            cab.get("total"),
            (det.get("loja") or {}).get("id"),
            (situacao or {}).get("id"),
            (situacao or {}).get("valor"),
            (contato or {}).get("id"),
            (vendedor or {}).get("id"),
            json.dumps(det, ensure_ascii=False),
        ),
    )
    cur.close()


def replace_itens(conn, pedido_id, det):
    cur = conn.cursor()
    cur.execute("delete from itens where pedido_id=%s", (pedido_id,))
    itens = det.get("itens") or []
    seq = 0
    for it in itens:
        item = it.get("item", it)
        seq += 1
        prod = item.get("produto") or {}
        prod_id = prod.get("id")
        try:
            prod_id = int(prod_id) if prod_id is not None else None
        except Exception:
            prod_id = None

        cur.execute(
            """
          insert into itens (
            pedido_id, seq, item_id_api, produto_id, produto_codigo, produto_nome,
            quantidade, valor_unitario, desconto, valor_total, updated_at, raw
          )
          values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(), %s)
        """,
            (
                int(pedido_id),
                seq,
                item.get("id"),
                prod_id,
                coalesce(prod.get("codigo"), item.get("codigo")),
                coalesce(prod.get("descricao"), prod.get("nome"), item.get("descricao")),
                item.get("quantidade") or item.get("qtde") or item.get("qtd"),
                item.get("valor") or item.get("valorUnitario") or item.get("preco"),
                item.get("desconto") or item.get("valorDesconto") or 0,
                item.get("total")
                or item.get("totalItem")
                or item.get("valorTotal")
                or ((item.get("valor") or 0) * (item.get("quantidade") or 0)),
                json.dumps(item, ensure_ascii=False),
            ),
        )
    cur.close()


# ---------- estado (waterline) ----------
def get_last_sync(conn):
    cur = conn.cursor()
    cur.execute("select value from etl_state where key='last_sync'")
    row = cur.fetchone()
    cur.close()
    if row and row[0]:
        return dt.datetime.fromisoformat(row[0]).date()
    return dt.date.today() - dt.timedelta(days=30)  # primeira execução: últimos 30 dias


def set_last_sync(conn, day: dt.date):
    cur = conn.cursor()
    cur.execute(
        """
      insert into etl_state (key, value)
      values ('last_sync', %s)
      on conflict (key) do update set value=excluded.value
    """,
        (day.isoformat(),),
    )
    cur.close()


# ---------- main ----------
def main():
    token = get_access_token()
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False

    try:
        last_sync = get_last_sync(conn)
        today = dt.date.today()

        params = {
            "limite": "100",
            # use o par suportado pelo seu endpoint; aqui usamos dataAlteracao
            "dataAlteracaoInicial": last_sync.isoformat(),
            "dataAlteracaoFinal": today.isoformat(),
        }

        print(f"Buscando pedidos alterados de {params['dataAlteracaoInicial']} a {params['dataAlteracaoFinal']}...")
        total = 0

        for cab in fetch_paged("pedidos/vendas", token, params):
            total += 1
            pid = int(cab.get("id"))
            time.sleep(0.15)  # segura <= 3 req/s somando listagem+detalhe
            det = api_get(f"pedidos/vendas/{pid}", token)
            data_det = det.get("data", det)

            ensure_dims(conn, data_det)
            upsert_pedido(conn, cab, data_det)
            replace_itens(conn, pid, data_det)
            conn.commit()  # commit por pedido

        set_last_sync(conn, today)
        conn.commit()
        print(f"OK! Pedidos processados: {total}")

    except Exception as e:
        conn.rollback()
        print("ERRO:", e, file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
