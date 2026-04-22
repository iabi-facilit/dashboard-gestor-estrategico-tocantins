import os
from contextlib import contextmanager
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
import psycopg2

DB_URL = os.getenv("DB_URL", "postgresql://bi:F%40c1l1t@172.19.4.156:15432/tocantins")
SCHEMA = "teletrabalho"

app = FastAPI()


@contextmanager
def get_db():
    conn = psycopg2.connect(DB_URL, options=f"-c search_path={SCHEMA}")
    try:
        yield conn
    finally:
        conn.close()


def build_where(
    status: str | None,
    situacao: str | None,
    orgao: str | None,
    participante: str | None,
    data_inicio: str | None,
    data_termino: str | None,
):
    clauses = ["template = 'false'"]
    params = []

    if status:
        clauses.append("status = %s")
        params.append(status)
    if situacao:
        clauses.append("statusinformado = %s")
        params.append(situacao)
    if orgao:
        clauses.append("agencyacronym = %s")
        params.append(orgao)
    if participante:
        clauses.append("responsavel = %s")
        params.append(participante)
    if data_inicio:
        clauses.append(
            "TO_DATE(SPLIT_PART(datainicioprevisto, ' ', 1), 'DD/MM/YYYY') >= TO_DATE(%s, 'YYYY-MM-DD')"
        )
        params.append(data_inicio)
    if data_termino:
        clauses.append(
            "TO_DATE(SPLIT_PART(dataterminoprevisto, ' ', 1), 'DD/MM/YYYY') <= TO_DATE(%s, 'YYYY-MM-DD')"
        )
        params.append(data_termino)

    where = "WHERE " + " AND ".join(clauses)
    return where, params


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/filtros")
def filtros():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT status FROM api_acaoprioritaria "
            "WHERE template='false' AND status IS NOT NULL ORDER BY status"
        )
        status = [r[0] for r in cur.fetchall()]

        cur.execute(
            "SELECT DISTINCT statusinformado FROM api_acaoprioritaria "
            "WHERE template='false' AND statusinformado IS NOT NULL AND statusinformado != '' "
            "ORDER BY statusinformado"
        )
        situacoes = [r[0] for r in cur.fetchall()]

        cur.execute(
            "SELECT DISTINCT agencyacronym FROM api_acaoprioritaria "
            "WHERE template='false' AND agencyacronym IS NOT NULL ORDER BY agencyacronym"
        )
        orgaos = [r[0] for r in cur.fetchall()]

        cur.execute(
            "SELECT DISTINCT responsavel FROM api_acaoprioritaria "
            "WHERE template='false' AND responsavel IS NOT NULL ORDER BY responsavel"
        )
        participantes = [r[0] for r in cur.fetchall()]

    return {
        "status": status,
        "situacoes": situacoes,
        "orgaos": orgaos,
        "participantes": participantes,
    }


@app.get("/api/kpis")
def kpis(
    status: str | None = Query(None),
    situacao: str | None = Query(None),
    orgao: str | None = Query(None),
    participante: str | None = Query(None),
    data_inicio: str | None = Query(None),
    data_termino: str | None = Query(None),
):
    where, params = build_where(status, situacao, orgao, participante, data_inicio, data_termino)
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                COUNT(DISTINCT uuid_)         AS total_planos,
                COUNT(DISTINCT agencyid)      AS total_orgaos,
                COUNT(DISTINCT responsavelid) AS total_participantes
            FROM api_acaoprioritaria {where}
            """,
            params,
        )
        row = cur.fetchone()
    return {
        "total_planos": row[0],
        "total_orgaos": row[1],
        "total_participantes": row[2],
    }


@app.get("/api/por-status")
def por_status(
    status: str | None = Query(None),
    situacao: str | None = Query(None),
    orgao: str | None = Query(None),
    participante: str | None = Query(None),
    data_inicio: str | None = Query(None),
    data_termino: str | None = Query(None),
):
    where, params = build_where(status, situacao, orgao, participante, data_inicio, data_termino)
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT status, status_cor, COUNT(DISTINCT uuid_) AS total
            FROM api_acaoprioritaria {where}
            GROUP BY status, status_cor
            ORDER BY total DESC
            """,
            params,
        )
        rows = cur.fetchall()
    return [{"status": r[0], "cor": r[1], "total": r[2]} for r in rows]


@app.get("/api/por-orgao")
def por_orgao(
    status: str | None = Query(None),
    situacao: str | None = Query(None),
    orgao: str | None = Query(None),
    participante: str | None = Query(None),
    data_inicio: str | None = Query(None),
    data_termino: str | None = Query(None),
):
    where, params = build_where(status, situacao, orgao, participante, data_inicio, data_termino)
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT agencyacronym, COUNT(DISTINCT uuid_) AS total
            FROM api_acaoprioritaria {where}
            GROUP BY agencyacronym
            ORDER BY total DESC
            LIMIT 20
            """,
            params,
        )
        rows = cur.fetchall()
    return [{"orgao": r[0], "total": r[1]} for r in rows]


# Serve o frontend estático
app.mount("/", StaticFiles(directory="/app/static", html=True), name="static")
